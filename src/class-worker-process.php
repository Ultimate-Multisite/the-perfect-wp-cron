<?php

namespace QueueWorker;

use Workerman\Worker;
use Workerman\Timer;

/**
 * Core worker logic for the Workerman event loop.
 *
 * Encapsulates all state and callbacks that were previously closures/globals
 * in bin/worker.php. Each Workerman child process gets its own instance.
 */
class Worker_Process
{
    /** @var string Absolute path to wp-load.php */
    private string $wp_load;

    /** @var string Primary site domain for WordPress bootstrap */
    private string $primary_domain;

    /** @var string Absolute path to execute-job.php */
    private string $execute_script;

    /** @var string Absolute path to scan-cron.php */
    private string $scan_script;

    // --- Configuration ---
    private int $max_concurrent;
    private int $max_batch_size;
    private int $as_max_concurrent;
    private int $as_max_batch_size;
    private array $as_lanes;
    private int $as_rescan_interval;
    private int $batch_timeout;
    private int $rescan_interval;
    private int $memory_limit;
    private int $uptime_limit;

    // --- Per-process state ---
    /** @var array<string, int> tracking_key => timer_id */
    private array $pending_timers = [];

    /** @var list<array{process: resource, pipes: array, payloads: list<Job_Payload>, started: int, stdout: string, stderr: string, lane: string}> */
    private array $running_processes = [];

    /** @var array<int, list<Job_Payload>> site_id => [payload, ...] */
    private array $pending_batch = [];

    /** @var array<string, array<int, list<Job_Payload>>> lane => site_id => [payload, ...] */
    private array $pending_as_batch = [];

    private int $running_jobs = 0;
    private int $start_time;
    private bool $is_rescanning = false;
    private int $last_rescan_started = 0;
    private int $last_rescan_finished = 0;
    private int $last_rescan_duration = 0;

    public function __construct(string $wp_load, string $primary_domain, string $execute_script, string $scan_script = '')
    {
        $this->wp_load        = $wp_load;
        $this->primary_domain = $primary_domain;
        $this->execute_script = $execute_script;
        $this->scan_script    = $scan_script;

        $this->max_concurrent  = Config::max_concurrent();
        $this->max_batch_size  = Config::max_batch_size();
        $this->as_max_concurrent = Config::action_scheduler_max_concurrent();
        $this->as_max_batch_size = Config::action_scheduler_max_batch_size();
        $this->as_lanes       = Config::action_scheduler_lanes();
        $this->as_rescan_interval = Config::action_scheduler_rescan_interval();
        $this->batch_timeout   = Config::batch_timeout();
        $this->rescan_interval = Config::rescan_interval();
        $this->memory_limit    = Config::memory_limit();
        $this->uptime_limit    = Config::uptime_limit();
        $this->start_time      = time();
    }

    /**
     * Workerman onWorkerStart callback.
     */
    public function on_worker_start(Worker $w): void
    {
        $this->start_time = time();
        $worker_id = $w->id;
        $socket_path = Config::socket_path();

        if ($worker_id === 0 && file_exists($socket_path)) {
            chmod($socket_path, 0660);
        }

        $this->bootstrap_wordpress();
        Worker::log(sprintf('[W%d] WordPress bootstrapped for scanning. Primary domain: %s', $worker_id, $this->primary_domain));

        self::ensure_lock_table();

        if ($worker_id === 0) {
            Job_Log::ensure_table();
        }

        // Batch flush timer — every 1 second. Action Scheduler gets its own
        // lane so due AS jobs are not starved behind noisy WP-Cron batches.
        Timer::add(1, fn() => $this->flush_batches());

        // Subprocess polling timer — every 0.5 seconds
        Timer::add(0.5, fn() => $this->poll_processes($worker_id));

        // Initial DB scan. Only one coordinator worker performs the expensive
        // full-network scan so large multisite networks do not block every
        // event loop child at startup.
        if ($this->is_rescan_coordinator($worker_id)) {
            Worker::log(sprintf('[W%d] Scanning database for pending jobs...', $worker_id));
            $this->run_full_rescan($worker_id);
            Worker::log(sprintf('[W%d] Loaded %d pending jobs.', $worker_id, count($this->pending_timers)));
        } else {
            Worker::log(sprintf('[W%d] Skipping full-network scan; worker 0 is rescan coordinator.', $worker_id));
        }

        // Periodic full-network rescan. Keep it on the coordinator worker only
        // so status commands and urgent Action Scheduler notifications can be
        // handled by other children while WP-Cron backlogs are being scanned.
        Timer::add($this->rescan_interval, function () use ($worker_id) {
            if (!$this->is_rescan_coordinator($worker_id)) {
                return;
            }
            Worker::log(sprintf('[W%d][RESCAN] %d timers pending, rescanning...', $worker_id, count($this->pending_timers)));
            $this->run_full_rescan($worker_id);
        });

        // Action Scheduler can miss instant socket notification when an action
        // is enqueued before its integration hook is registered in a request.
        // Keep a short AS-only safety scan in the current/root context; the
        // full multisite rescan still covers per-site and sovereign tables.
        Timer::add($this->as_rescan_interval, function () use ($worker_id) {
            Worker::log(sprintf('[W%d][AS_RESCAN] scanning pending Action Scheduler jobs...', $worker_id));
            $this->rescan_action_scheduler_jobs();
        });

        $stagger = $worker_id * 5;
        if ($stagger > 0) {
            Timer::add($stagger, function () {}, null, false);
        }

        // Memory and uptime watchdog
        Timer::add(30, function () use ($worker_id) {
            $this->check_limits($worker_id);
        });
    }

    /**
     * Workerman onMessage callback.
     */
    public function on_message($connection, string $data): void
    {
        $data = trim($data);

        $decoded = json_decode($data, true);
        if (is_array($decoded) && isset($decoded['command'])) {
            $this->handle_command($connection, $decoded);
            return;
        }

        try {
            $payload = Job_Payload::from_json($data);
            $this->schedule_timer($payload);
        } catch (\Throwable $e) {
            Worker::log('[SOCKET] Invalid payload: ' . $e->getMessage());
        }

        $connection->close();
    }

    // ------------------------------------------------------------------
    // Private methods
    // ------------------------------------------------------------------

    private function bootstrap_wordpress(): void
    {
        $_SERVER['HTTP_HOST']      = $this->primary_domain;
        $_SERVER['SERVER_NAME']    = $this->primary_domain;
        $_SERVER['REQUEST_URI']    = '/';
        $_SERVER['SERVER_PORT']    = '443';
        $_SERVER['HTTPS']          = 'on';
        $_SERVER['REQUEST_METHOD'] = 'GET';

        require_once $this->wp_load;
    }

    /**
     * Schedule a one-shot timer for a job payload.
     */
    private function schedule_timer(Job_Payload $payload): void
    {
        $key = $payload->tracking_key();
        if (isset($this->pending_timers[$key])) {
            return;
        }
        $delay = max(0, $payload->timestamp - time());
        $timer_id = Timer::add($delay, fn($p) => $this->execute_job($p), [$payload], false);
        $this->pending_timers[$key] = $timer_id;
    }

    /**
     * Fired by timer when a job is due. Claims the job and adds to pending batch.
     */
    private function execute_job(Job_Payload $payload): void
    {
        $key = $payload->tracking_key();
        unset($this->pending_timers[$key]);

        if ($payload->source === 'action_scheduler') {
            $this->execute_action_scheduler_job($payload);
            return;
        }

        // Check concurrency limit before claiming
        if (count($this->running_processes) >= $this->max_concurrent) {
            // Re-schedule with 2s delay
            $timer_id = Timer::add(2, fn($p) => $this->execute_job($p), [$payload], false);
            $this->pending_timers[$key] = $timer_id;
            return;
        }

        // Atomically claim — only one worker process wins
        $lock_key = 'qw_' . substr(md5($key), 0, 40);
        if (!$this->claim_job($lock_key)) {
            return;
        }

        // Collect into pending batch — flushed by the batch timer
        $this->pending_batch[$payload->site_id][] = $payload;
    }

    /**
     * Fired by timer when an Action Scheduler job is due.
     *
     * Action Scheduler gets a dedicated lane instead of sharing the normal
     * WP-Cron concurrency budget. This preserves exact scheduling for urgent
     * AS work (checkout pending-site publish, domain stages, billing jobs)
     * even when the worker is draining many recurring cron events.
     */
    private function execute_action_scheduler_job(Job_Payload $payload): void
    {
        $key = $payload->tracking_key();

        $lane = $this->action_scheduler_lane_for($payload);
        if ($this->running_process_count($lane) >= $this->lane_max_concurrent($lane)) {
            $timer_id = Timer::add(1, fn($p) => $this->execute_action_scheduler_job($p), [$payload], false);
            $this->pending_timers[$key] = $timer_id;
            return;
        }

        $lock_key = 'qw_' . substr(md5($key), 0, 40);
        if (!$this->claim_job($lock_key)) {
            return;
        }

        $this->pending_as_batch[$lane][$payload->site_id][] = $payload;
    }

    /**
     * Spawn a subprocess to execute a batch of jobs (all same site).
     */
    private function spawn_batch(array $payloads, int $worker_id, string $lane = 'wp_cron'): void
    {
        $json_array = array_map(
            fn($p) => json_decode($p->to_json(), true),
            $payloads
        );
        $json_data = json_encode($json_array);
        $cmd = sprintf('php %s --stdin', escapeshellarg($this->execute_script));

        $process = proc_open($cmd, [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ], $pipes);

        if (!is_resource($process)) {
            Worker::log(sprintf(
                '[W%d][ERROR] Failed to spawn subprocess for batch of %d jobs on site %d',
                $worker_id,
                count($payloads),
                $payloads[0]->site_id
            ));
            return;
        }

        // Write JSON payload to stdin, then close
        fwrite($pipes[0], $json_data);
        fclose($pipes[0]);
        stream_set_blocking($pipes[1], false);
        stream_set_blocking($pipes[2], false);

        $this->running_jobs++;
        $this->running_processes[] = [
            'process'  => $process,
            'pipes'    => $pipes,
            'payloads' => $payloads,
            'started'  => time(),
            'stdout'   => '',
            'stderr'   => '',
            'lane'     => $lane,
        ];

        $hooks = array_map(fn($p) => $p->hook, $payloads);
        Worker::log(sprintf(
            '[W%d][SPAWN][%s] Batch: %d jobs on site %d (pid %d, %d running): %s',
            $worker_id,
            $lane,
            count($payloads),
            $payloads[0]->site_id,
            proc_get_status($process)['pid'] ?? 0,
            $this->running_jobs,
            implode(', ', array_unique($hooks))
        ));
    }

    /**
     * Flush pending batches — takes up to max_batch_size per site.
     */
    private function flush_batches(): void
    {
        $this->flush_action_scheduler_batches();

        foreach ($this->pending_batch as $site_id => $payloads) {
            if (empty($payloads)) {
                unset($this->pending_batch[$site_id]);
                continue;
            }
            if (count($this->running_processes) >= $this->max_concurrent) {
                break;
            }
            $batch = array_splice($this->pending_batch[$site_id], 0, $this->max_batch_size);
            // Worker ID isn't critical for batch flush logging; use 0
            $this->spawn_batch($batch, 0);
            if (empty($this->pending_batch[$site_id])) {
                unset($this->pending_batch[$site_id]);
            }
        }
    }

    /**
     * Flush pending Action Scheduler batches before normal WP-Cron batches.
     */
    private function flush_action_scheduler_batches(): void
    {
        foreach ($this->pending_as_batch as $lane => $site_batches) {
            foreach ($site_batches as $site_id => $payloads) {
                if (empty($payloads)) {
                    unset($this->pending_as_batch[$lane][$site_id]);
                    continue;
                }
                if ($this->running_process_count($lane) >= $this->lane_max_concurrent($lane)) {
                    break;
                }
                $batch = array_splice($this->pending_as_batch[$lane][$site_id], 0, $this->lane_max_batch_size($lane));
                $this->spawn_batch($batch, 0, $lane);
                if (empty($this->pending_as_batch[$lane][$site_id])) {
                    unset($this->pending_as_batch[$lane][$site_id]);
                }
            }

            if (empty($this->pending_as_batch[$lane])) {
                unset($this->pending_as_batch[$lane]);
            }
        }
    }

    private function action_scheduler_lane_for(Job_Payload $payload): string
    {
        foreach ($this->as_lanes as $lane) {
            if (!$this->lane_matches($lane, $payload)) {
                continue;
            }

            return $lane['name'];
        }

        return 'action_scheduler';
    }

    private function lane_matches(array $lane, Job_Payload $payload): bool
    {
        if (!empty($lane['sites']) && !in_array($payload->site_id, $lane['sites'], true)) {
            return false;
        }
        if (!empty($lane['groups']) && !in_array($payload->group, $lane['groups'], true)) {
            return false;
        }
        if (!empty($lane['hooks']) && !in_array($payload->hook, $lane['hooks'], true)) {
            return false;
        }

        return true;
    }

    private function lane_max_concurrent(string $lane_name): int
    {
        foreach ($this->as_lanes as $lane) {
            if ($lane['name'] === $lane_name) {
                return (int) $lane['max_concurrent'];
            }
        }

        return $this->as_max_concurrent;
    }

    private function lane_max_batch_size(string $lane_name): int
    {
        foreach ($this->as_lanes as $lane) {
            if ($lane['name'] === $lane_name) {
                return (int) $lane['max_batch_size'];
            }
        }

        return $this->as_max_batch_size;
    }

    private function running_process_count(string $lane): int
    {
        $count = 0;
        foreach ($this->running_processes as $proc) {
            if (($proc['lane'] ?? 'wp_cron') === $lane) {
                $count++;
            }
        }
        return $count;
    }

    /**
     * Poll running subprocesses for completion or timeout.
     */
    private function poll_processes(int $worker_id): void
    {
        foreach ($this->running_processes as $i => $proc) {
            // Read available output
            $out = stream_get_contents($proc['pipes'][1]);
            if ($out !== false && $out !== '') {
                $this->running_processes[$i]['stdout'] .= $out;
            }
            $err = stream_get_contents($proc['pipes'][2]);
            if ($err !== false && $err !== '') {
                $this->running_processes[$i]['stderr'] .= $err;
            }

            $status = proc_get_status($proc['process']);

            if (!$status['running']) {
                // Read remaining output
                $remaining = stream_get_contents($proc['pipes'][1]);
                if ($remaining) {
                    $this->running_processes[$i]['stdout'] .= $remaining;
                }
                $remaining_err = stream_get_contents($proc['pipes'][2]);
                if ($remaining_err) {
                    $this->running_processes[$i]['stderr'] .= $remaining_err;
                }

                fclose($proc['pipes'][1]);
                fclose($proc['pipes'][2]);
                proc_close($proc['process']);

                $exit_code = $status['exitcode'];
                $payloads  = $proc['payloads'];
                $elapsed   = time() - $proc['started'];
                $site_id   = $payloads[0]->site_id;
                $count     = count($payloads);

                if ($exit_code === 0) {
                    Worker::log(sprintf(
                        '[W%d][DONE] Batch: %d jobs on site %d (%ds)',
                        $worker_id,
                        $count,
                        $site_id,
                        $elapsed
                    ));
                } else {
                    $error_msg = trim($this->running_processes[$i]['stderr'] ?: $this->running_processes[$i]['stdout']);
                    if (strlen($error_msg) > 500) {
                        $error_msg = substr($error_msg, 0, 500) . '...';
                    }
                    Worker::log(sprintf(
                        '[W%d][FAIL] Batch: %d jobs on site %d (exit %d, %ds): %s',
                        $worker_id,
                        $count,
                        $site_id,
                        $exit_code,
                        $elapsed,
                        $error_msg
                    ));
                }

                unset($this->running_processes[$i]);
                $this->running_jobs--;
                continue;
            }

            // Timeout check
            if (time() - $proc['started'] > $this->batch_timeout) {
                $payloads = $proc['payloads'];
                $pid = $status['pid'];
                proc_terminate($proc['process'], 9);
                fclose($proc['pipes'][1]);
                fclose($proc['pipes'][2]);
                proc_close($proc['process']);

                Worker::log(sprintf(
                    '[W%d][TIMEOUT] Batch: %d jobs on site %d exceeded %ds limit (pid %d)',
                    $worker_id,
                    count($payloads),
                    $payloads[0]->site_id,
                    $this->batch_timeout,
                    $pid
                ));

                unset($this->running_processes[$i]);
                $this->running_jobs--;
            }
        }
        // Re-index to prevent gaps
        $this->running_processes = array_values($this->running_processes);
    }

    /**
     * Scan all multisite blogs for pending WP Cron events and AS actions.
     */
    private function rescan_all_jobs(): void
    {
        $sites = get_sites(['number' => 0, 'fields' => 'ids']);
        $sovereign_sites = $this->sovereign_site_entries();

        $current_blog_id = get_current_blog_id();

        foreach ($sites as $site_id) {
            $site_id = (int) $site_id;
            if (isset($sovereign_sites[$site_id])) {
                continue;
            }

            switch_to_blog($site_id);

            // Flush stale object cache so we read fresh data from DB.
            wp_cache_delete('cron', 'options');
            wp_cache_delete('alloptions', 'options');

            // Action Scheduler can store actions in either the root tables or
            // per-site tables. Only scan the currently switched site when its
            // Action Scheduler tables exist so missing legacy tables do not
            // produce noisy database errors or duplicate root-site timers.
            $this->rescan_action_scheduler_jobs();

            // Scan WP Cron
            $crons = _get_cron_array();
            if (is_array($crons)) {
                $seen_cron_signatures = [];
                foreach ($crons as $timestamp => $hooks) {
                    if (!is_array($hooks)) {
                        continue;
                    }
                    foreach ($hooks as $hook => $events) {
                        if (in_array($hook, [
                            'wp_version_check',
                            'wp_update_plugins',
                            'wp_update_themes',
                            'action_scheduler_run_queue',
                            'action_scheduler_run_cleanup',
                        ], true)) {
                            continue;
                        }

                        foreach ($events as $event) {
                            $signature = $this->cron_event_signature($hook, $event, (int) $timestamp);
                            if (isset($seen_cron_signatures[$signature])) {
                                continue;
                            }
                            $seen_cron_signatures[$signature] = true;

                            $event_obj = (object) array_merge($event, [
                                'hook'      => $hook,
                                'timestamp' => $timestamp,
                            ]);
                            $payload = Job_Payload::from_cron_event($event_obj);
                            $this->schedule_timer($payload);
                        }
                    }
                }
            }
            restore_current_blog();
        }

        foreach ($sovereign_sites as $site_id => $entry) {
            $this->rescan_sovereign_site_jobs((int) $site_id, $entry);
        }

        if ($current_blog_id !== get_current_blog_id()) {
            switch_to_blog($current_blog_id);
        }
    }

    private function is_rescan_coordinator(int $worker_id): bool
    {
        return $worker_id === 0;
    }

    private function run_full_rescan(int $worker_id): void
    {
        if ($this->is_rescanning) {
            Worker::log(sprintf('[W%d][RESCAN] Previous full-network rescan still running; skipping overlap.', $worker_id));
            return;
        }

        $this->is_rescanning = true;
        $this->last_rescan_started = time();

        try {
            $this->rescan_all_jobs();
        } finally {
            $this->last_rescan_finished = time();
            $this->last_rescan_duration = max(0, $this->last_rescan_finished - $this->last_rescan_started);
            $this->is_rescanning = false;
        }
    }

    /**
     * Load sovereign tenant entries from the generated multi-tenancy registry.
     *
     * Sovereign tenants must be scanned in a fresh PHP process bootstrapped with
     * their own domain. switch_to_blog() in the root worker would target
     * wp_<blog_id>_* tables inside the tenant DB, but sovereign tenant tables use
     * the tenant-local wp_ prefix.
     *
     * @return array<int, array>
     */
    private function sovereign_site_entries(): array
    {
        if (!defined('WP_CONTENT_DIR')) {
            return [];
        }

        $path = WP_CONTENT_DIR . '/site-registry.data.json';
        if (!is_readable($path)) {
            return [];
        }

        $json = file_get_contents($path);
        $data = json_decode($json ?: '', true);
        if (!is_array($data) || empty($data['sites']) || !is_array($data['sites'])) {
            return [];
        }

        $entries = [];
        foreach ($data['sites'] as $site_id => $entry) {
            if (!is_array($entry)) {
                continue;
            }
            if (($entry['isolation_model'] ?? '') !== 'sovereign') {
                continue;
            }
            if (($entry['status'] ?? 'active') !== 'active') {
                continue;
            }
            if (empty($entry['domains']) || !is_array($entry['domains'])) {
                continue;
            }

            $entries[(int) $site_id] = $entry;
        }

        return $entries;
    }

    private function rescan_sovereign_site_jobs(int $site_id, array $entry): void
    {
        if ($this->scan_script === '' || !file_exists($this->scan_script)) {
            Worker::log(sprintf('[RESCAN][SOVEREIGN][ERROR] Missing scan script for site %d', $site_id));
            return;
        }

        $site_url = $this->site_url_from_registry_entry($entry);
        if ($site_url === '') {
            Worker::log(sprintf('[RESCAN][SOVEREIGN][ERROR] Missing tenant domain for site %d', $site_id));
            return;
        }

        $cmd = sprintf('php %s --stdin', escapeshellarg($this->scan_script));
        $process = proc_open($cmd, [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ], $pipes);

        if (!is_resource($process)) {
            Worker::log(sprintf('[RESCAN][SOVEREIGN][ERROR] Failed to spawn scan for site %d', $site_id));
            return;
        }

        fwrite($pipes[0], json_encode([
            'site_id'  => $site_id,
            'site_url' => $site_url,
        ]));
        fclose($pipes[0]);

        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $exit_code = proc_close($process);

        if ($exit_code !== 0) {
            $message = trim((string) $stderr);
            Worker::log(sprintf('[RESCAN][SOVEREIGN][FAIL] Site %d scan failed: %s', $site_id, $message));
            return;
        }

        $payloads = json_decode((string) $stdout, true);
        if (!is_array($payloads)) {
            Worker::log(sprintf('[RESCAN][SOVEREIGN][FAIL] Site %d scan returned invalid JSON', $site_id));
            return;
        }

        foreach ($payloads as $payload_data) {
            if (!is_array($payload_data)) {
                continue;
            }
            $this->schedule_timer(new Job_Payload($payload_data));
        }
    }

    private function site_url_from_registry_entry(array $entry): string
    {
        foreach ($entry['domains'] ?? [] as $domain) {
            $domain = trim((string) $domain);
            if ($domain !== '') {
                return 'https://' . $domain . '/';
            }
        }

        return '';
    }

    private function rescan_action_scheduler_jobs(): void
    {
        if (!function_exists('as_get_scheduled_actions')) {
            return;
        }

        if (!$this->action_scheduler_tables_exist()) {
            return;
        }

        try {
            $actions = as_get_scheduled_actions([
                'status'   => \ActionScheduler_Store::STATUS_PENDING,
                'per_page' => 500,
            ]);
        } catch (\Throwable $e) {
            Worker::log(sprintf(
                '[RESCAN][ACTION_SCHEDULER][FAIL] Site %d scan failed: %s',
                get_current_blog_id(),
                $e->getMessage()
            ));
            return;
        }

        foreach ($actions as $action_id => $action) {
            $payload = Job_Payload::from_as_action($action_id);
            if ($payload) {
                $this->schedule_timer($payload);
            }
        }
    }

    private function action_scheduler_tables_exist(): bool
    {
        global $wpdb;

        foreach (['actions', 'groups'] as $suffix) {
            $table = $wpdb->prefix . 'actionscheduler_' . $suffix;
            $found = $wpdb->get_var($wpdb->prepare('SHOW TABLES LIKE %s', $wpdb->esc_like($table)));
            if ($found !== $table) {
                return false;
            }
        }

        return true;
    }

    /**
     * Atomically claim a job via INSERT IGNORE into the lock table.
     */
    private function claim_job(string $job_key): bool
    {
        global $wpdb;
        $table = $wpdb->base_prefix . 'qw_job_locks';

        $wpdb->query("DELETE FROM `$table` WHERE claimed_at < DATE_SUB(NOW(), INTERVAL 5 MINUTE)");

        $result = $wpdb->query($wpdb->prepare(
            "INSERT IGNORE INTO `$table` (lock_key, claimed_at) VALUES (%s, NOW())",
            $job_key
        ));

        return $result === 1;
    }

    /**
     * Handle incoming socket commands (status, restart).
     */
    private function handle_command($connection, array $cmd): void
    {
        switch ($cmd['command']) {
            case 'status':
                $mem_mb = memory_get_usage(true) / 1024 / 1024;
                $uptime = time() - $this->start_time;
                $hours  = floor($uptime / 3600);
                $mins   = floor(($uptime % 3600) / 60);
                $secs   = $uptime % 60;

                $running_details = [];
                foreach ($this->running_processes as $proc) {
                    $hooks = [];
                    $site_id = 0;
                    $count = 0;
                    if (!empty($proc['payloads'])) {
                        $site_id = $proc['payloads'][0]->site_id;
                        $count = count($proc['payloads']);
                        foreach ($proc['payloads'] as $p) {
                            $hooks[$p->hook] = true;
                        }
                    }
                    $running_details[] = [
                        'hook'    => implode(', ', array_keys($hooks)),
                        'site_id' => $site_id,
                        'count'   => $count,
                        'elapsed' => time() - $proc['started'],
                        'lane'    => $proc['lane'] ?? 'wp_cron',
                    ];
                }

                $response = json_encode([
                    'pid'             => getmypid(),
                    'uptime'          => sprintf('%dh %dm %ds', $hours, $mins, $secs),
                    'uptime_seconds'  => $uptime,
                    'pending_timers'  => count($this->pending_timers),
                    'pending_as_batches' => $this->pending_action_scheduler_count(),
                    'running_jobs'    => $this->running_jobs,
                    'running_as_jobs' => $this->running_action_scheduler_count(),
                    'running_as_lanes' => $this->running_action_scheduler_counts(),
                    'pending_as_lanes' => $this->pending_action_scheduler_counts(),
                    'memory'          => sprintf('%.1f MB', $mem_mb),
                    'running_details' => $running_details,
                    'rescan'          => [
                        'in_progress' => $this->is_rescanning,
                        'last_started' => $this->last_rescan_started,
                        'last_finished' => $this->last_rescan_finished,
                        'last_duration' => $this->last_rescan_duration,
                    ],
                ]);
                $connection->close($response . "\n");
                break;

            case 'restart':
                Worker::log('[COMMAND] Restart requested.');
                $connection->close();
                Worker::stopAll();
                break;

            default:
                $connection->close();
                break;
        }
    }

    private function pending_action_scheduler_count(): int
    {
        return array_sum($this->pending_action_scheduler_counts());
    }

    private function pending_action_scheduler_counts(): array
    {
        $counts = [];
        foreach ($this->pending_as_batch as $lane => $site_batches) {
            $counts[$lane] = array_sum(array_map('count', $site_batches));
        }

        return $counts;
    }

    private function running_action_scheduler_counts(): array
    {
        $counts = [];
        foreach ($this->running_processes as $proc) {
            $lane = $proc['lane'] ?? 'wp_cron';
            if ($lane === 'wp_cron') {
                continue;
            }
            $counts[$lane] = ($counts[$lane] ?? 0) + 1;
        }

        return $counts;
    }

    private function running_action_scheduler_count(): int
    {
        return array_sum($this->running_action_scheduler_counts());
    }

    private function cron_event_signature(string $hook, array $event, int $timestamp): string
    {
        $event_timestamp = empty($event['schedule']) ? $timestamp : 0;

        return sprintf(
            '%s:%s:%d:%s',
            $hook,
            $event['schedule'] ?? '',
            $event_timestamp,
            md5(serialize($event['args'] ?? []))
        );
    }

    /**
     * Check memory and uptime limits, trigger restart if exceeded.
     */
    private function check_limits(int $worker_id): void
    {
        $mem_mb = memory_get_usage(true) / 1024 / 1024;
        $uptime = time() - $this->start_time;

        if ($mem_mb > $this->memory_limit) {
            Worker::log(sprintf('[W%d][WATCHDOG] Memory %.1fMB > %dMB limit. Restarting.', $worker_id, $mem_mb, $this->memory_limit));
            Worker::stopAll();
        }

        if ($uptime > $this->uptime_limit) {
            Worker::log(sprintf('[W%d][WATCHDOG] Uptime %ds > %ds limit. Restarting.', $worker_id, $uptime, $this->uptime_limit));
            Worker::stopAll();
        }
    }

    /**
     * Create the job locks table if it doesn't exist.
     */
    public static function ensure_lock_table(): void
    {
        global $wpdb;
        $table = $wpdb->base_prefix . 'qw_job_locks';

        $wpdb->query("CREATE TABLE IF NOT EXISTS `$table` (
            lock_key VARCHAR(64) NOT NULL PRIMARY KEY,
            claimed_at DATETIME NOT NULL
        ) ENGINE=InnoDB"
        );
    }
}
