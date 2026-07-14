<?php

namespace QueueWorker;

/**
 * Executes a batch of job payloads inside a WordPress environment.
 *
 * Spawned by Worker_Process in a subprocess. Each job gets a per-job
 * SIGALRM timeout and results are logged to the qw_job_log table.
 */
class Job_Executor
{
    private int $site_id;
    private int $job_timeout;
    private bool $has_pcntl;

    public function __construct(int $site_id)
    {
        $this->site_id     = $site_id;
        $this->job_timeout = Config::job_timeout();
        $this->has_pcntl   = function_exists('pcntl_async_signals');

        if ($this->has_pcntl) {
            pcntl_async_signals(true);
        }
    }

    /**
     * Execute all payloads and return an exit code.
     *
     * @param list<array> $payloads Raw decoded payload arrays.
     * @return int 0 if all succeeded, 1 if any failed.
     */
    public function run(array $payloads): int
    {
        $exit_code = 0;
        $results   = [];

        foreach ($payloads as $i => $job) {
            $source    = $job['source'] ?? 'wp_cron';
            $hook      = $job['hook'];
            $job_start = microtime(true);
            $scheduled = (int) ($job['timestamp'] ?? 0);
            $status    = 'ok';
            $error     = null;

            $this->set_alarm();

            try {
                if ($source === 'action_scheduler') {
                    $this->execute_action_scheduler($job);
                    $results[] = ['status' => 'ok', 'type' => 'as', 'action_id' => (int) ($job['action_id'] ?? 0), 'hook' => $hook];
                } else {
                    $this->execute_wp_cron($job);
                    $results[] = ['status' => 'ok', 'type' => 'cron', 'hook' => $hook];
                }
            } catch (\Throwable $e) {
                $msg    = $e->getMessage();
                $status = (stripos($msg, 'timeout') !== false) ? 'timeout' : 'error';
                $error  = $msg;
                fwrite(STDERR, "[Job $i] {$hook}: {$msg}\n");
                $results[] = ['status' => 'error', 'hook' => $hook, 'error' => $msg];
                $exit_code = 1;
            }

            $this->cancel_alarm();

            $job_completed = microtime(true);
            $duration_ms   = (int) round(($job_completed - $job_start) * 1000);
            $wait_ms       = $scheduled > 0 ? max(0, (int) round(($job_start - $scheduled) * 1000)) : null;
            Job_Log::insert(
                (int) ($job['site_id'] ?? $this->site_id),
                $hook,
                $source,
                $status,
                $duration_ms,
                $error,
                $scheduled > 0 ? gmdate('Y-m-d H:i:s', $scheduled) : null,
                gmdate('Y-m-d H:i:s', (int) $job_start),
                gmdate('Y-m-d H:i:s', (int) $job_completed),
                $wait_ms,
                $duration_ms,
                (string) ($job['lane'] ?? ($source === 'action_scheduler' ? 'action_scheduler' : 'wp_cron')),
                !empty($job['action_id']) ? (int) $job['action_id'] : null
            );
        }

        echo json_encode($results);
        return $exit_code;
    }

    private function execute_wp_cron(array $job): void
    {
        $timestamp = (int) ($job['timestamp'] ?? 0);
        $hook      = $job['hook'];
        $args      = $job['args'] ?? [];
        $schedule  = $job['schedule'] ?? '';

        // The long-running worker can hold stale one-shot payloads after a job
        // was already removed by another worker, deploy, or native cron pass.
        // Re-read the cron option and only fire callbacks that still exist in
        // WordPress' cron array. This prevents duplicate email/report jobs when
        // an old payload is re-scanned or re-claimed after the short lock TTL.
        $this->flush_cron_option_cache();
        $crons = _get_cron_array();
        $event_key = $this->find_cron_event_key($crons, $timestamp, $hook, $args);
        if ($event_key === null) {
            return;
        }

        // Reschedule recurring events before firing (mirrors wp-cron.php behavior).
        // Without this, the event is simply deleted and plugins re-register it
        // at time() on next load, causing an infinite rapid-fire loop.
        if ($schedule !== '') {
            // A previous attempt can have successfully created the next
            // occurrence but stopped before removing this stale row. WordPress
            // then returns false because writing the identical next event is a
            // no-op. Remove the stale duplicate without replaying its callback.
            if ($this->recurring_successor_exists($crons, $timestamp, $hook, $args, $schedule, $event_key)) {
                if (!$this->unschedule_cron_event($timestamp, $hook, $args)) {
                    throw new \RuntimeException(sprintf('Failed to remove duplicate cron event %s at %d', $hook, $timestamp));
                }

                return;
            }

            $rescheduled = wp_reschedule_event($timestamp, $schedule, $hook, $args, true);

            if ($rescheduled !== true) {
                $reason = is_wp_error($rescheduled) ? $rescheduled->get_error_message() : 'unknown error';
                throw new \RuntimeException(sprintf('Failed to reschedule cron event %s at %d: %s', $hook, $timestamp, $reason));
            }
        }

        $unscheduled = $this->unschedule_cron_event($timestamp, $hook, $args);

        if (!$unscheduled) {
            throw new \RuntimeException(sprintf('Failed to unschedule cron event %s at %d', $hook, $timestamp));
        }

        do_action_ref_array($hook, $args);
    }

    private function flush_cron_option_cache(): void
    {
        if (!function_exists('wp_cache_delete')) {
            return;
        }

        wp_cache_delete('cron', 'options');
        wp_cache_delete('alloptions', 'options');
    }

    private function unschedule_cron_event(int $timestamp, string $hook, array $args): bool
    {
        $unscheduled = wp_unschedule_event($timestamp, $hook, $args);

        if ($unscheduled !== false) {
            return true;
        }

        // Some cron arrays can contain malformed event keys that do not match
        // WordPress' md5( serialize( $args ) ) convention. In that state
        // wp_unschedule_event() cannot remove the row even though the event is
        // present, so remove the matching hook/args manually before firing.
        $crons = _get_cron_array();
        $key = $this->find_cron_event_key($crons, $timestamp, $hook, $args);

        if ($key === null) {
            return false;
        }

        unset($crons[$timestamp][$hook][$key]);

        if (empty($crons[$timestamp][$hook])) {
            unset($crons[$timestamp][$hook]);
        }

        if (empty($crons[$timestamp])) {
            unset($crons[$timestamp]);
        }

        return _set_cron_array($crons) !== false;
    }

    private function recurring_successor_exists(array $crons, int $timestamp, string $hook, array $args, string $schedule, string $event_key): bool
    {
        $event = $crons[$timestamp][$hook][$event_key] ?? null;
        if (!is_array($event)) {
            return false;
        }

        $interval = (int) ($event['interval'] ?? 0);
        if ($interval < 1) {
            $schedules = wp_get_schedules();
            $interval = (int) ($schedules[$schedule]['interval'] ?? 0);
        }

        if ($interval < 1) {
            return false;
        }

        $now = time();
        $next_timestamp = $timestamp >= $now
            ? $now + $interval
            : $now + ($interval - (($now - $timestamp) % $interval));
        $next_key = $this->find_cron_event_key($crons, $next_timestamp, $hook, $args);

        if ($next_key === null) {
            return false;
        }

        $next_event = $crons[$next_timestamp][$hook][$next_key] ?? null;

        return is_array($next_event) && ($next_event['schedule'] ?? '') === $schedule;
    }

    private function find_cron_event_key($crons, int $timestamp, string $hook, array $args): ?string
    {
        if ($timestamp < 1 || $hook === '') {
            return null;
        }

        if (!is_array($crons) || empty($crons[$timestamp][$hook]) || !is_array($crons[$timestamp][$hook])) {
            return null;
        }

        $key = md5(serialize($args));
        if (isset($crons[$timestamp][$hook][$key])) {
            return $key;
        }

        foreach ($crons[$timestamp][$hook] as $event_key => $event) {
            if (($event['args'] ?? []) === $args) {
                return (string) $event_key;
            }
        }

        return null;
    }

    private function execute_action_scheduler(array $job): void
    {
        $action_id = (int) ($job['action_id'] ?? 0);
        if (!$action_id || !class_exists('ActionScheduler_QueueRunner')) {
            throw new \RuntimeException('ActionScheduler not available or missing action_id');
        }
        $runner = \ActionScheduler_QueueRunner::instance();
        $runner->process_action($action_id);
    }

    private function set_alarm(): void
    {
        if (!$this->has_pcntl) {
            return;
        }
        pcntl_signal(SIGALRM, function () {
            throw new \RuntimeException('Per-job timeout exceeded');
        });
        pcntl_alarm($this->job_timeout);
    }

    private function cancel_alarm(): void
    {
        if (!$this->has_pcntl) {
            return;
        }
        pcntl_alarm(0);
    }
}
