<?php

namespace Workerman {
    class Worker
    {
        public int $id = 0;
        public static array $logs = [];

        public static function log(string $message): void
        {
            self::$logs[] = $message;
        }

        public static function stopAll(): void
        {
        }
    }

    class Timer
    {
        public static int $next_id = 1;
        public static array $delays = [];

        public static function add($delay, callable $callback, array $args = [], bool $persistent = true): int
        {
            self::$delays[] = $delay;

            return self::$next_id++;
        }
    }
}

namespace WP_CLI\Utils {
    function format_items(string $format, array $items, array $fields): void
    {
    }
}

namespace {
    class WP_CLI
    {
        public static array $errors = [];
        public static array $logs = [];
        public static array $successes = [];

        public static function error(string $message): void
        {
            self::$errors[] = $message;
            throw new RuntimeException($message);
        }

        public static function warning(string $message): void
        {
            self::$logs[] = 'WARNING: ' . $message;
        }

        public static function log(string $message): void
        {
            self::$logs[] = $message;
        }

        public static function success(string $message): void
        {
            self::$successes[] = $message;
        }
    }

    class Test_Connection
    {
        public bool $closed = false;
        public ?string $response = null;

        public function close(?string $response = null): void
        {
            $this->closed = true;
            $this->response = $response;
        }
    }

    class Test_WP_Error
    {
        private string $code;
        private string $message;

        public function __construct(string $code, string $message)
        {
            $this->code    = $code;
            $this->message = $message;
        }

        public function get_error_code(): string
        {
            return $this->code;
        }

        public function get_error_message(): string
        {
            return $this->message;
        }
    }

    class ActionScheduler_Store
    {
        public const STATUS_PENDING = 'pending';
    }

    class Test_ActionScheduler_Store
    {
        public array $statuses = [];
        public array $action_ids = [];

        public function get_status(int $action_id): string
        {
            $this->action_ids[] = $action_id;
            $status = array_shift($this->statuses);

            if ($status instanceof Throwable) {
                throw $status;
            }

            return (string) $status;
        }
    }

    class ActionScheduler
    {
        public static ?Test_ActionScheduler_Store $store = null;

        public static function store(): Test_ActionScheduler_Store
        {
            return self::$store ??= new Test_ActionScheduler_Store();
        }
    }

    class ActionScheduler_QueueRunner
    {
        public static ?self $runner = null;
        public array $processed_action_ids = [];

        public static function instance(): self
        {
            return self::$runner ??= new self();
        }

        public function process_action(int $action_id): void
        {
            $this->processed_action_ids[] = $action_id;
        }
    }
}

namespace {
    use QueueWorker\Config;
    use QueueWorker\Cron_Event_Filter;
    use QueueWorker\Cron_Interceptor;
    use QueueWorker\Job_Log;
    use QueueWorker\Job_Payload;
    use QueueWorker\Socket_Client;
    use QueueWorker\Worker_Process;

    $_SERVER['HTTP_HOST'] = 'example.test';

    $GLOBALS['test_crons'] = [];
    $GLOBALS['test_current_blog_id'] = 1;
    $GLOBALS['test_sites'] = [1];
    $GLOBALS['test_site_network_ids'] = [];
    $GLOBALS['test_filters'] = [];
    $GLOBALS['test_actions'] = [];
    $GLOBALS['test_switched_blogs'] = [];
    $GLOBALS['test_unscheduled_events'] = [];
    $GLOBALS['test_rescheduled_events'] = [];
    $GLOBALS['test_reschedule_result'] = true;
    $GLOBALS['test_schedules'] = [
        'hourly' => ['interval' => 3600],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_cache_deletes'] = [];
    $GLOBALS['test_ms_switched'] = false;
    $GLOBALS['test_site_url'] = 'https://example.test';
    $GLOBALS['test_site_domain'] = 'example.test';
    $GLOBALS['test_site_path'] = '/';
    $GLOBALS['test_mapping_table_exists'] = false;
    $GLOBALS['test_mapping_row'] = null;
    $GLOBALS['test_canonical_mapping_conflicts'] = 0;

    $worker_entrypoint = file_get_contents(__DIR__ . '/../bin/worker.php');
    assert_true(is_string($worker_entrypoint), 'Worker entrypoint must be readable');
    assert_true(
        str_contains($worker_entrypoint, 'QueueWorker\\Bootstrap::discover_wp_load'),
        'Worker entrypoint must fully qualify Bootstrap before namespace imports are declared'
    );
    assert_true(
        str_contains($worker_entrypoint, "require_once dirname(__DIR__) . '/src/class-bootstrap.php'"),
        'Worker entrypoint must load Bootstrap directly when plugin-local vendor autoload is absent from dist installs'
    );

    $executor_entrypoint = file_get_contents(__DIR__ . '/../bin/execute-job.php');
    assert_true(is_string($executor_entrypoint), 'Executor entrypoint must be readable');
    assert_true(
        str_contains($executor_entrypoint, "define('QUEUE_WORKER_EXECUTOR_RUNNING', true)"),
        'Executor subprocesses must identify themselves before WordPress loads'
    );

    $scanner_entrypoint = file_get_contents(__DIR__ . '/../bin/scan-cron.php');
    assert_true(is_string($scanner_entrypoint), 'Scanner entrypoint must be readable');
    $scanner_discovery = strpos($scanner_entrypoint, 'QueueWorker\\Bootstrap::discover_wp_load');
    $scanner_abspath = strpos($scanner_entrypoint, "define('ABSPATH'");
    $scanner_site_autoload = strpos($scanner_entrypoint, 'require_once $site_autoload;');
    assert_true(
        $scanner_discovery !== false
            && $scanner_abspath !== false
            && $scanner_site_autoload !== false
            && $scanner_discovery < $scanner_site_autoload
            && $scanner_abspath < $scanner_site_autoload,
        'Scanner must discover WordPress and define ABSPATH before loading the site autoloader'
    );

    $plugin_entrypoint = file_get_contents(__DIR__ . '/../the-perfect-wp-cron.php');
    assert_true(is_string($plugin_entrypoint), 'Plugin entrypoint must be readable');
    assert_true(
        str_contains($plugin_entrypoint, 'if ($queue_worker_running && !$job_executor_running)'),
        'Only non-executor worker processes may skip cron interceptor registration'
    );
    assert_true(
        strpos($plugin_entrypoint, "add_action('init', ['QueueWorker\\\\Cron_Interceptor', 'register'])")
            < strpos($plugin_entrypoint, 'if ($job_executor_running)'),
        'Executor subprocesses must register scheduling interceptors before stopping normal plugin bootstrap'
    );
    assert_true(
        str_contains($plugin_entrypoint, "add_filter('wu_wp_cron_status_override', ['QueueWorker\\\\Socket_Client', 'filter_wp_cron_status_override'])"),
        'Plugin bootstrap must register the Ultimate Multisite cron health override'
    );

    class Test_WPDB
    {
        public string $prefix = 'wp_';
        public string $base_prefix = 'wp_';
        public array $queries = [];
        public array $query_results = [];
        public array $cron_site_locks = [];
        public array $expired_cron_site_locks = [];

        public function prepare(string $query, ...$args): string
        {
            return vsprintf(str_replace('%s', "'%s'", $query), $args);
        }

        public function esc_like(string $text): string
        {
            return $text;
        }

        public function get_var(string $query): ?string
        {
            if (preg_match('/SELECT owner_token FROM `wp_qw_cron_site_locks` WHERE site_id = ([0-9]+)/', $query, $matches)) {
                return $this->cron_site_locks[(int) $matches[1]] ?? null;
            }
            if (str_contains($query, 'SHOW TABLES LIKE') && str_contains($query, 'wu_domain_mappings')) {
                return $GLOBALS['test_mapping_table_exists'] ? 'wp_wu_domain_mappings' : null;
            }
            if (str_contains($query, 'COUNT(*)') && str_contains($query, 'wu_domain_mappings')) {
                return (string) $GLOBALS['test_canonical_mapping_conflicts'];
            }
            return null;
        }

        public function get_row(string $query): ?object
        {
            unset($query);
            return $GLOBALS['test_mapping_row'];
        }

        public function query(string $query)
        {
            $this->queries[] = $query;

            if (str_contains($query, 'qw_cron_site_locks')) {
                if (str_starts_with($query, 'CREATE TABLE')) {
                    return 1;
                }
                if (preg_match("/INSERT INTO `wp_qw_cron_site_locks` .*VALUES \\(([0-9]+), '([a-f0-9]+)'/", $query, $matches)) {
                    $site_id = (int) $matches[1];
                    if (isset($this->cron_site_locks[$site_id]) && empty($this->expired_cron_site_locks[$site_id])) {
                        // Emulate a client using CLIENT_FOUND_ROWS, where a
                        // no-op duplicate-key update can report one row.
                        return 1;
                    }
                    $this->cron_site_locks[$site_id] = $matches[2];
                    unset($this->expired_cron_site_locks[$site_id]);
                    return 1;
                }
                if (preg_match("/UPDATE `wp_qw_cron_site_locks` .*WHERE site_id = ([0-9]+) AND owner_token = '([a-f0-9]+)'/", $query, $matches)) {
                    $site_id = (int) $matches[1];
                    return ($this->cron_site_locks[$site_id] ?? '') === $matches[2] ? 1 : 0;
                }
                if (preg_match("/DELETE FROM `wp_qw_cron_site_locks` WHERE site_id = ([0-9]+) AND owner_token = '([a-f0-9]+)'/", $query, $matches)) {
                    $site_id = (int) $matches[1];
                    if (($this->cron_site_locks[$site_id] ?? '') !== $matches[2]) {
                        return 0;
                    }
                    unset($this->cron_site_locks[$site_id]);
                    return 1;
                }
            }

            return $this->query_results === [] ? 1 : array_shift($this->query_results);
        }
    }

    $GLOBALS['wpdb'] = new Test_WPDB();

    function get_site_url(): string
    {
        return $GLOBALS['test_site_url'];
    }

    function get_site(int $site_id): object
    {
        return (object) [
            'domain'  => $GLOBALS['test_site_domain'],
            'path'    => $GLOBALS['test_site_path'],
            'site_id' => $GLOBALS['test_site_network_ids'][$site_id] ?? 1,
        ];
    }

    function get_current_blog_id(): int
    {
        return (int) $GLOBALS['test_current_blog_id'];
    }

    function is_multisite(): bool
    {
        return false;
    }

    function ms_is_switched(): bool
    {
        return (bool) $GLOBALS['test_ms_switched'];
    }

    function wp_get_schedules(): array
    {
        return $GLOBALS['test_schedules'];
    }

    function get_sites(array $args): array
    {
        unset($args);

        return $GLOBALS['test_sites'];
    }

    function switch_to_blog(int $site_id): void
    {
        $GLOBALS['test_current_blog_id'] = $site_id;
        $GLOBALS['test_switched_blogs'][] = $site_id;
        $GLOBALS['test_ms_switched'] = true;
    }

    function restore_current_blog(): void
    {
        $GLOBALS['test_current_blog_id'] = 1;
        $GLOBALS['test_ms_switched'] = false;
    }

    function wp_cache_delete(string $key, string $group): void
    {
        $GLOBALS['test_cache_deletes'][] = [
            'key'   => $key,
            'group' => $group,
        ];
    }

    function add_filter(string $hook_name, callable $callback): void
    {
        $GLOBALS['test_filters'][] = [
            'hook'     => $hook_name,
            'callback' => $callback,
        ];
    }

    function add_action(string $hook_name, callable $callback): void
    {
        $GLOBALS['test_actions'][] = [
            'hook'     => $hook_name,
            'callback' => $callback,
        ];
    }

    function _get_cron_array(): array
    {
        return $GLOBALS['test_crons'];
    }

    function _set_cron_array(array $crons): bool
    {
        $GLOBALS['test_crons'] = $crons;

        return true;
    }

    function wp_unschedule_event(int $timestamp, string $hook, array $args = []): bool
    {
        $key = md5(serialize($args));

        if (!isset($GLOBALS['test_crons'][$timestamp][$hook][$key])) {
            return false;
        }

        $GLOBALS['test_unscheduled_events'][] = [
            'timestamp' => $timestamp,
            'hook'      => $hook,
            'args'      => $args,
        ];

        unset($GLOBALS['test_crons'][$timestamp][$hook][$key]);

        if (empty($GLOBALS['test_crons'][$timestamp][$hook])) {
            unset($GLOBALS['test_crons'][$timestamp][$hook]);
        }

        if (empty($GLOBALS['test_crons'][$timestamp])) {
            unset($GLOBALS['test_crons'][$timestamp]);
        }

        return true;
    }

    function wp_reschedule_event(int $timestamp, string $schedule, string $hook, array $args = [], bool $wp_error = false)
    {
        unset($wp_error);
        $GLOBALS['test_rescheduled_events'][] = [
            'timestamp' => $timestamp,
            'schedule'  => $schedule,
            'hook'      => $hook,
            'args'      => $args,
        ];

        if ($GLOBALS['test_reschedule_result'] === true && !isset($GLOBALS['test_schedules'][$schedule])) {
            return new Test_WP_Error('invalid_schedule', 'The schedule does not exist.');
        }

        return $GLOBALS['test_reschedule_result'];
    }

    function is_wp_error($thing): bool
    {
        return $thing instanceof Test_WP_Error;
    }

    function do_action_ref_array(string $hook, array $args): void
    {
        $GLOBALS['test_fired_actions'][] = [
            'hook' => $hook,
            'args' => $args,
        ];
    }

    function assert_true(bool $condition, string $message): void
    {
        if (!$condition) {
            throw new RuntimeException($message);
        }
    }

    function assert_same($expected, $actual, string $message): void
    {
        if ($expected !== $actual) {
            throw new RuntimeException($message . ' Expected ' . var_export($expected, true) . ', got ' . var_export($actual, true));
        }
    }

    function private_property(object $object, string $property)
    {
        $reflection = new ReflectionProperty($object, $property);
        $reflection->setAccessible(true);

        return $reflection->getValue($object);
    }

    function set_private_property(object $object, string $property, mixed $value): void
    {
        $reflection = new ReflectionProperty($object, $property);
        $reflection->setAccessible(true);
        $reflection->setValue($object, $value);
    }

    function invoke_private(object $object, string $method, array $args = [])
    {
        $reflection = new ReflectionMethod($object, $method);
        $reflection->setAccessible(true);

        return $reflection->invokeArgs($object, $args);
    }

    require_once __DIR__ . '/../src/class-config.php';
    require_once __DIR__ . '/../src/class-cron-event-filter.php';
    require_once __DIR__ . '/../src/class-cron-interceptor.php';
    require_once __DIR__ . '/../src/class-cli-commands.php';
    require_once __DIR__ . '/../src/class-job-log.php';
    require_once __DIR__ . '/../src/class-job-payload.php';
    require_once __DIR__ . '/../src/class-job-executor.php';
    require_once __DIR__ . '/../src/class-socket-client.php';
    require_once __DIR__ . '/../src/class-worker-process.php';

    $payload = new Job_Payload([
        'site_id'   => 7,
        'site_url'  => 'https://tenant.example.test',
        'hook'      => 'as_hook',
        'args'      => ['order_id' => 123],
        'timestamp' => 1710000000,
        'source'    => 'action_scheduler',
        'action_id' => 44,
        'group'     => 'checkout',
    ]);
    $decoded = json_decode($payload->to_json(), true);
    assert_same('checkout', $decoded['group'], 'Action Scheduler group must be serialized');
    assert_same('action_scheduler:7:44', $payload->tracking_key(), 'Action Scheduler identity must use its durable action ID');
    $same_action_different_route = new Job_Payload([
        'site_id'   => 7,
        'site_url'  => 'https://alternate.example.test',
        'hook'      => 'changed_transport_hook',
        'args'      => ['changed' => true],
        'timestamp' => 1710000100,
        'source'    => 'action_scheduler',
        'action_id' => 44,
        'group'     => 'alternate',
    ]);
    assert_same($payload->tracking_key(), $same_action_different_route->tracking_key(), 'Action Scheduler identity must not depend on routing metadata');
    $different_action = new Job_Payload([
        'site_id'   => 7,
        'site_url'  => 'https://tenant.example.test',
        'hook'      => 'as_hook',
        'args'      => ['order_id' => 123],
        'timestamp' => 1710000000,
        'source'    => 'action_scheduler',
        'action_id' => 45,
        'group'     => 'checkout',
    ]);
    assert_true($payload->tracking_key() !== $different_action->tracking_key(), 'Distinct Action Scheduler actions must not collapse');

    $cron_route_a = new Job_Payload([
        'site_id'   => 7,
        'site_url'  => 'https://tenant.example.test',
        'hook'      => 'route_independent_hook',
        'args'      => ['batch' => 1],
        'timestamp' => 2147483647,
        'schedule'  => 'hourly',
        'source'    => 'wp_cron',
    ]);
    $cron_route_b = new Job_Payload([
        'site_id'   => 7,
        'site_url'  => 'https://mapped.example.test',
        'hook'      => 'route_independent_hook',
        'args'      => ['batch' => 1],
        'timestamp' => 2147483647,
        'schedule'  => 'hourly',
        'source'    => 'wp_cron',
    ]);
    assert_same($cron_route_a->tracking_key(), $cron_route_b->tracking_key(), 'WP-Cron identity must not depend on its bootstrap URL');

    putenv('QUEUE_WORKER_SCHEDULING_HORIZON=2147483647');
    $failed_bootstrap_file = tempnam(sys_get_temp_dir(), 'qw-bootstrap-failure-');
    assert_true(false !== $failed_bootstrap_file, 'Bootstrap failure fixture must be created');
    assert_true(
        false !== file_put_contents($failed_bootstrap_file, "<?php throw new \\RuntimeException('intentional bootstrap failure');"),
        'Bootstrap failure fixture must be writable'
    );
    $failed_start_worker = new Worker_Process($failed_bootstrap_file, 'example.test', __FILE__);
    $failed_start_worker->on_worker_start(new \Workerman\Worker());
    assert_same(false, private_property($failed_start_worker, 'is_ready'), 'A bootstrap failure must leave the child running but unavailable');
    assert_true(private_property($failed_start_worker, 'failure_reason') !== '', 'A bootstrap failure must retain an actionable reason');
    $failed_start_status_connection = new Test_Connection();
    $failed_start_worker->on_message($failed_start_status_connection, json_encode(['command' => 'status']));
    $failed_start_status = json_decode((string) $failed_start_status_connection->response, true);
    assert_same(false, $failed_start_status['ready'] ?? null, 'An unavailable worker must report its readiness state');
    assert_true(($failed_start_status['failure_reason'] ?? '') !== '', 'An unavailable worker status response must report its failure reason');
    $failed_start_job_connection = new Test_Connection();
    $failed_start_worker->on_message($failed_start_job_connection, $payload->to_json());
    assert_true($failed_start_job_connection->closed, 'An unavailable worker must reject queued jobs without crashing');
    $failed_start_job_response = json_decode((string) $failed_start_job_connection->response, true);
    assert_same(false, $failed_start_job_response['accepted'] ?? null, 'An unavailable worker must explicitly reject queued jobs');
    assert_same('worker_unavailable', $failed_start_job_response['reason'] ?? null, 'An unavailable worker rejection must identify its state');
    assert_true(unlink($failed_bootstrap_file), 'Bootstrap failure fixture must be removed');

    putenv('QUEUE_WORKER_AS_LANES=' . json_encode([
        [
            'name' => 'checkout_lane',
            'sites' => [7],
            'groups' => ['checkout'],
            'hooks' => ['as_hook'],
            'max_concurrent' => 2,
            'max_batch_size' => 3,
        ],
    ]));
    $worker = new Worker_Process(__FILE__, 'example.test', __FILE__);
    $identity_worker = new Worker_Process(__FILE__, 'example.test', __FILE__);
    invoke_private($identity_worker, 'schedule_timer', [$cron_route_a]);
    invoke_private($identity_worker, 'schedule_timer', [$cron_route_b]);
    assert_same(1, count(private_property($identity_worker, 'pending_timers')), 'Route variants of one WP-Cron event must collapse to one timer');
    $horizon_worker = new Worker_Process(__FILE__, 'example.test', __FILE__);
    set_private_property($horizon_worker, 'scheduling_horizon', 60);
    invoke_private($horizon_worker, 'schedule_timer', [new Job_Payload([
        'site_id'   => 7,
        'site_url'  => 'https://tenant.example.test',
        'hook'      => 'within_horizon_hook',
        'args'      => [],
        'timestamp' => time() + 30,
        'source'    => 'wp_cron',
    ])]);
    invoke_private($horizon_worker, 'schedule_timer', [new Job_Payload([
        'site_id'   => 7,
        'site_url'  => 'https://tenant.example.test',
        'hook'      => 'beyond_horizon_hook',
        'args'      => [],
        'timestamp' => time() + 3600,
        'source'    => 'wp_cron',
    ])]);
    assert_same(
        1,
        count(private_property($horizon_worker, 'pending_timers')),
        'The worker must not retain timers beyond its bounded scheduling horizon'
    );

    Worker_Process::ensure_lock_table();
    assert_true(
        str_contains(implode("\n", $GLOBALS['wpdb']->queries), 'CREATE TABLE IF NOT EXISTS `wp_qw_cron_site_locks`'),
        'Worker startup must provision the shared per-site cron lock table'
    );
    $site_7_owner = invoke_private($worker, 'claim_cron_site_lock', [7]);
    assert_true(is_string($site_7_owner) && $site_7_owner !== '', 'The first worker must claim a site cron lease');
    $second_worker = new Worker_Process(__FILE__, 'example.test', __FILE__);
    assert_same(null, invoke_private($second_worker, 'claim_cron_site_lock', [7]), 'A second worker must not claim an active lease for the same site');
    $site_8_owner = invoke_private($second_worker, 'claim_cron_site_lock', [8]);
    assert_true(is_string($site_8_owner) && $site_8_owner !== '', 'Different sites must retain parallel cron execution');
    assert_same(true, invoke_private($worker, 'refresh_cron_site_lock', [7, $site_7_owner]), 'The owning worker must refresh its cron site lease');
    $GLOBALS['wpdb']->expired_cron_site_locks[7] = true;
    $replacement_site_7_owner = invoke_private($second_worker, 'claim_cron_site_lock', [7]);
    assert_true(
        is_string($replacement_site_7_owner) && $replacement_site_7_owner !== $site_7_owner,
        'Another worker must be able to replace an expired site lease'
    );
    assert_same(false, invoke_private($worker, 'refresh_cron_site_lock', [7, $site_7_owner]), 'A stale owner must not refresh a replaced site lease');
    invoke_private($worker, 'release_cron_site_lock', [7, $site_7_owner]);
    assert_same(null, invoke_private($worker, 'claim_cron_site_lock', [7]), 'A stale owner must not release another worker\'s site lease');

    $lease_process = proc_open(PHP_BINARY . ' -r ' . escapeshellarg('sleep(5);'), [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ], $lease_pipes);
    assert_true(is_resource($lease_process), 'Lease-renewal failure fixture must start a subprocess');
    fclose($lease_pipes[0]);
    stream_set_blocking($lease_pipes[1], false);
    stream_set_blocking($lease_pipes[2], false);
    set_private_property($worker, 'running_processes', [[
        'process'                  => $lease_process,
        'pipes'                    => $lease_pipes,
        'payloads'                 => [$payload],
        'started'                  => time(),
        'stdout'                   => '',
        'stderr'                   => '',
        'lane'                     => 'wp_cron',
        'cron_site_lock_owner'     => $site_7_owner,
        'cron_site_lock_refreshed' => time() - 61,
    ]]);
    set_private_property($worker, 'running_jobs', 1);
    invoke_private($worker, 'poll_processes', [0]);
    assert_same([], private_property($worker, 'running_processes'), 'A batch must be reaped when its cron site lease cannot be renewed');
    assert_same(0, private_property($worker, 'running_jobs'), 'A reaped batch must no longer count as running');
    assert_same($replacement_site_7_owner, $GLOBALS['wpdb']->cron_site_locks[7] ?? null, 'A stale worker must not delete the replacement site lease');

    invoke_private($second_worker, 'release_cron_site_lock', [7, $replacement_site_7_owner]);
    $reclaimed_site_7_owner = invoke_private($worker, 'claim_cron_site_lock', [7]);
    assert_true(is_string($reclaimed_site_7_owner) && $reclaimed_site_7_owner !== '', 'A completed batch must release the site for the next worker');
    invoke_private($worker, 'release_cron_site_lock', [7, $reclaimed_site_7_owner]);
    invoke_private($second_worker, 'release_cron_site_lock', [8, $site_8_owner]);

    assert_same('checkout_lane', invoke_private($worker, 'action_scheduler_lane_for', [$payload]), 'AS lane must match by site, group, and hook');
    assert_same('action_scheduler', invoke_private($worker, 'action_scheduler_lane_for', [new Job_Payload([
        'site_id' => 7,
        'site_url' => 'https://tenant.example.test',
        'hook' => 'other_hook',
        'args' => [],
        'timestamp' => 1710000000,
        'source' => 'action_scheduler',
        'group' => 'checkout',
    ])]), 'AS lane must not match when hook differs');

    $scan_fixture = tempnam(sys_get_temp_dir(), 'qw-sovereign-scan-');
    assert_true(false !== $scan_fixture, 'Sovereign scan fixture must be created');
    $sovereign_entry = ['domains' => ['tenant.example.test']];
    $sovereign_worker = new Worker_Process(__FILE__, 'example.test', __FILE__, $scan_fixture);

    assert_true(
        false !== file_put_contents($scan_fixture, "<?php fwrite(STDOUT, \"bootstrap notice\\n[]\");"),
        'Noisy sovereign scan fixture must be writable'
    );
    \Workerman\Worker::$logs = [];
    invoke_private($sovereign_worker, 'rescan_sovereign_site_jobs', [7, $sovereign_entry]);
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), 'returned invalid JSON'),
        'Noisy subprocess stdout must be rejected instead of scheduling partial jobs'
    );
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), 'stdout_bytes='),
        'Invalid subprocess JSON diagnostics must be bounded to metadata'
    );

    assert_true(
        false !== file_put_contents($scan_fixture, '<?php fwrite(STDERR, "sensitive bootstrap detail"); exit(7);'),
        'Failed sovereign scan fixture must be writable'
    );
    \Workerman\Worker::$logs = [];
    invoke_private($sovereign_worker, 'rescan_sovereign_site_jobs', [7, $sovereign_entry]);
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), 'exited with code 7'),
        'Failed subprocesses must report their exit status'
    );
    assert_true(
        !str_contains(implode("\n", \Workerman\Worker::$logs), 'sensitive bootstrap detail'),
        'Failed subprocess diagnostics must not expose stderr contents'
    );

    assert_true(
        false !== file_put_contents($scan_fixture, '<?php'),
        'Empty-output sovereign scan fixture must be writable'
    );
    \Workerman\Worker::$logs = [];
    invoke_private($sovereign_worker, 'rescan_sovereign_site_jobs', [7, $sovereign_entry]);
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), 'returned empty output'),
        'Empty subprocess output must be distinguishable from invalid JSON'
    );

    assert_true(
        false !== file_put_contents($scan_fixture, '<?php fwrite(STDOUT, "[null]");'),
        'Invalid-shape sovereign scan fixture must be writable'
    );
    \Workerman\Worker::$logs = [];
    invoke_private($sovereign_worker, 'rescan_sovereign_site_jobs', [7, $sovereign_entry]);
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), 'invalid payload shape'),
        'Malformed payload arrays must not schedule partial jobs'
    );

    assert_true(
        false !== file_put_contents($scan_fixture, '<?php fwrite(STDOUT, "[]");'),
        'Empty-list sovereign scan fixture must be writable'
    );
    \Workerman\Worker::$logs = [];
    invoke_private($sovereign_worker, 'rescan_sovereign_site_jobs', [7, $sovereign_entry]);
    assert_same([], \Workerman\Worker::$logs, 'A valid empty sovereign scan must not be reported as a failure');

    assert_true(
        false !== file_put_contents($scan_fixture, '<?php fwrite(STDOUT, "[{\"site_id\":7,\"site_url\":\"https://tenant.example.test\",\"hook\":\"sovereign_hook\",\"args\":[],\"timestamp\":2147483647,\"source\":\"wp_cron\"}]");'),
        'Valid sovereign scan fixture must be writable'
    );
    \Workerman\Timer::$delays = [];
    \Workerman\Worker::$logs = [];
    invoke_private($sovereign_worker, 'rescan_sovereign_site_jobs', [7, $sovereign_entry]);
    assert_same(1, count(\Workerman\Timer::$delays), 'Valid sovereign payload arrays must continue to schedule normally');
    assert_same([], \Workerman\Worker::$logs, 'Valid sovereign payload arrays must not be reported as failures');

    $scan_script = file_get_contents(__DIR__ . '/../bin/scan-cron.php');
    assert_true(is_string($scan_script), 'Sovereign scan script must be readable');
    assert_true(
        str_contains($scan_script, '$stdout_buffer_level = ob_get_level();') && str_contains($scan_script, 'JSON_THROW_ON_ERROR'),
        'Sovereign scan script must isolate bootstrap output and fail safely when encoding JSON'
    );
    assert_true(unlink($scan_fixture), 'Sovereign scan fixture must be removed');
    assert_true(
        str_contains($scan_script, 'qw_scan_full_network_jobs($scheduling_horizon, $scan_timeout)')
            && str_contains($scan_script, 'qw_scan_registry_jobs([')
            && str_contains($scan_script, '$deadline = time() + $scheduling_horizon;'),
        'The scanner subprocess must enumerate the full network and reject jobs beyond the scheduling horizon'
    );

    $async_scan_fixture = tempnam(sys_get_temp_dir(), 'qw-async-scan-');
    assert_true(false !== $async_scan_fixture, 'Asynchronous scan fixture must be created');
    $async_scan_output = json_encode([[
        'site_id'   => 7,
        'site_url'  => 'https://tenant.example.test',
        'hook'      => 'async_scan_hook',
        'args'      => [],
        'timestamp' => time() + 30,
        'source'    => 'wp_cron',
    ]]);
    assert_true(false !== file_put_contents(
        $async_scan_fixture,
        '<?php usleep(1000000); fwrite(STDOUT, ' . var_export($async_scan_output, true) . ');'
    ), 'Asynchronous scan fixture must be writable');
    $async_scan_worker = new Worker_Process(__FILE__, 'example.test', __FILE__, $async_scan_fixture);
    set_private_property($async_scan_worker, 'scheduling_horizon', 60);
    \Workerman\Timer::$delays = [];
    \Workerman\Worker::$logs = [];
    $async_scan_started = microtime(true);
    invoke_private($async_scan_worker, 'run_full_rescan', [0]);
    assert_true(microtime(true) - $async_scan_started < 0.5, 'Starting a full-network scan must not block the event loop');
    assert_same(true, private_property($async_scan_worker, 'is_rescanning'), 'An active scanner subprocess must be visible in worker state');
    invoke_private($async_scan_worker, 'run_full_rescan', [0]);
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), 'skipping overlap'),
        'A periodic tick must not overlap an active full-network scan'
    );
    $async_scan_deadline = microtime(true) + 3;
    while (private_property($async_scan_worker, 'is_rescanning') && microtime(true) < $async_scan_deadline) {
        usleep(20000);
        invoke_private($async_scan_worker, 'poll_processes', [0]);
    }
    assert_same(false, private_property($async_scan_worker, 'is_rescanning'), 'The scanner must finish through non-blocking process polling');
    assert_same(1, count(private_property($async_scan_worker, 'pending_timers')), 'A completed scanner must schedule validated in-horizon jobs');

    assert_true(false !== file_put_contents($async_scan_fixture, '<?php sleep(5); fwrite(STDOUT, "[]");'), 'Timeout and shutdown scan fixture must be writable');
    $timeout_scan_worker = new Worker_Process(__FILE__, 'example.test', __FILE__, $async_scan_fixture);
    set_private_property($timeout_scan_worker, 'scan_timeout', 1);
    \Workerman\Worker::$logs = [];
    invoke_private($timeout_scan_worker, 'run_full_rescan', [0]);
    $active_timeout_scan = private_property($timeout_scan_worker, 'active_scan_process');
    $active_timeout_scan['started'] = time() - 2;
    set_private_property($timeout_scan_worker, 'active_scan_process', $active_timeout_scan);
    invoke_private($timeout_scan_worker, 'poll_processes', [0]);
    assert_same(null, private_property($timeout_scan_worker, 'active_scan_process'), 'An expired scanner must be terminated and reaped');
    assert_same(false, private_property($timeout_scan_worker, 'is_rescanning'), 'An expired scanner must clear the rescan state');
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), '[RESCAN][TIMEOUT]'),
        'An expired scanner must emit a bounded timeout diagnostic'
    );

    $shutdown_scan_worker = new Worker_Process(__FILE__, 'example.test', __FILE__, $async_scan_fixture);
    invoke_private($shutdown_scan_worker, 'run_full_rescan', [0]);
    $shutdown_scan_worker->on_worker_stop();
    assert_same(null, private_property($shutdown_scan_worker, 'active_scan_process'), 'Worker shutdown must terminate and reap the scanner subprocess');
    assert_true(unlink($async_scan_fixture), 'Asynchronous scan fixture must be removed');

    \Workerman\Timer::$delays = [];
    $registry_content_dir = sys_get_temp_dir() . '/qw-regression-content-' . getmypid();
    if (!is_dir($registry_content_dir)) {
        assert_true(mkdir($registry_content_dir, 0777, true), 'Registry fixture directory must be created');
    }
    if (!defined('WP_CONTENT_DIR')) {
        define('WP_CONTENT_DIR', $registry_content_dir);
    }
    assert_true(false !== file_put_contents(WP_CONTENT_DIR . '/site-registry.data.json', json_encode([
        'domain_index' => [
            'example.test' => 7,
        ],
    ])), 'Registry fixture must be writable');

    $isolated_site_1 = Job_Payload::isolated_site_id(49, 1);
    $isolated_site_2 = Job_Payload::isolated_site_id(49, 2);
    $other_network_site_1 = Job_Payload::isolated_site_id(50, 1);
    assert_same(210453397505, $isolated_site_1, 'Isolated queue identity must combine the network and local blog IDs deterministically');
    assert_true($isolated_site_1 !== $isolated_site_2, 'Local blog IDs must not collide inside one isolated network');
    assert_true($isolated_site_1 !== $other_network_site_1, 'Identical local blog IDs must not collide across isolated networks');
    assert_true($isolated_site_1 !== 1, 'Isolated queue identities must not collide with ordinary root blog IDs');

    $isolated_payload = new Job_Payload([
        'site_id'             => $isolated_site_1,
        'isolated_network_id' => 49,
        'local_site_id'       => 1,
        'site_url'            => 'https://isolated.example.test',
        'hook'                => 'isolated_hook',
        'timestamp'           => 2147483647,
    ]);
    assert_true($isolated_payload->routes_to_isolated_network(49), 'Isolated payload metadata must validate against its composite queue identity');
    assert_same(49, json_decode($isolated_payload->to_json(), true)['isolated_network_id'] ?? null, 'Isolated routing metadata must survive queue serialization');
    try {
        new Job_Payload([
            'site_id'             => $isolated_site_1,
            'isolated_network_id' => 50,
            'local_site_id'       => 1,
        ]);
        throw new RuntimeException('Mismatched isolated payload metadata must fail closed');
    } catch (InvalidArgumentException $exception) {
        assert_true(str_contains($exception->getMessage(), 'does not match'), 'Mismatched isolated payload metadata must report its identity failure');
    }

    assert_true(false !== file_put_contents(WP_CONTENT_DIR . '/network-registry.data.json', json_encode([
        'networks' => [
            49 => ['tier' => 'isolated', 'status' => 'active', 'domain' => 'isolated.example.test'],
            50 => ['tier' => 'shared', 'status' => 'active', 'domain' => 'shared.example.test'],
            51 => ['tier' => 'isolated', 'status' => 'inactive', 'domain' => 'inactive.example.test'],
            52 => ['tier' => 'isolated', 'status' => 'active'],
        ],
    ])), 'Network registry fixture must be writable');
    $registry_worker = new Worker_Process(__FILE__, 'example.test', __FILE__);
    assert_same([49], array_keys(invoke_private($registry_worker, 'isolated_network_entries')), 'Only active, routable isolated network entries may be scanned');
    assert_true(false !== file_put_contents(WP_CONTENT_DIR . '/network-registry.data.json', json_encode([
        'networks' => [
            49 => ['tier' => 'isolated', 'status' => 'active', 'domain' => 'isolated.example.test'],
            53 => ['tier' => 'isolated', 'status' => 'active', 'domains' => ['new-isolated.example.test']],
        ],
    ])), 'Updated network registry fixture must be writable');
    assert_same([49, 53], array_keys(invoke_private($registry_worker, 'isolated_network_entries')), 'Every full-rescan lookup must re-read newly registered isolated networks');
    assert_true(false !== file_put_contents(WP_CONTENT_DIR . '/network-registry.data.json', json_encode([
        'networks' => [
            12 => ['tier' => 'isolated', 'status' => 'active', 'domain' => 'isolated.example.test'],
        ],
    ])), 'Final network registry fixture must be writable');

    $isolated_scan_fixture = tempnam(sys_get_temp_dir(), 'qw-isolated-scan-');
    assert_true(false !== $isolated_scan_fixture, 'Isolated scan fixture must be created');
    $mismatched_scan_output = json_encode([[
        'site_id'             => Job_Payload::isolated_site_id(50, 1),
        'isolated_network_id' => 50,
        'local_site_id'       => 1,
        'site_url'            => 'https://wrong-isolated.example.test',
        'hook'                => 'wrong_isolated_hook',
        'timestamp'           => 2147483647,
    ]]);
    assert_true(false !== file_put_contents(
        $isolated_scan_fixture,
        '<?php fwrite(STDOUT, ' . var_export($mismatched_scan_output, true) . ');'
    ), 'Mismatched isolated scan fixture must be writable');
    $isolated_worker = new Worker_Process(__FILE__, 'example.test', __FILE__, $isolated_scan_fixture);
    \Workerman\Worker::$logs = [];
    \Workerman\Timer::$delays = [];
    invoke_private($isolated_worker, 'rescan_isolated_network_jobs', [49, ['domain' => 'isolated.example.test']]);
    assert_same([], \Workerman\Timer::$delays, 'Scanner payloads routed to another isolated network must not be scheduled');
    assert_true(
        str_contains(implode("\n", \Workerman\Worker::$logs), 'did not route to the expected isolated network'),
        'Rejected isolated scanner payloads must identify the route mismatch'
    );

    $valid_scan_output = json_encode([json_decode($isolated_payload->to_json(), true)]);
    assert_true(false !== file_put_contents(
        $isolated_scan_fixture,
        '<?php fwrite(STDOUT, ' . var_export($valid_scan_output, true) . ');'
    ), 'Valid isolated scan fixture must be writable');
    \Workerman\Worker::$logs = [];
    \Workerman\Timer::$delays = [];
    invoke_private($isolated_worker, 'rescan_isolated_network_jobs', [49, ['domain' => 'isolated.example.test']]);
    assert_same(1, count(\Workerman\Timer::$delays), 'Valid isolated scanner payloads must be scheduled');

    $proxy_network_payload = new Job_Payload([
        'site_id'             => Job_Payload::isolated_site_id(12, 1),
        'isolated_network_id' => 12,
        'local_site_id'       => 1,
        'site_url'            => 'https://isolated.example.test',
        'hook'                => 'proxy_network_hook',
        'timestamp'           => 2147483647,
    ]);
    $proxy_scan_output = json_encode([json_decode($proxy_network_payload->to_json(), true)]);
    assert_true(false !== file_put_contents(
        $isolated_scan_fixture,
        '<?php fwrite(STDOUT, ' . var_export($proxy_scan_output, true) . ');'
    ), 'Proxy-network scan fixture must be writable');
    $GLOBALS['test_sites'] = [1, 175];
    $GLOBALS['test_site_network_ids'] = [
        1   => 1,
        175 => 12,
    ];
    $GLOBALS['test_switched_blogs'] = [];
    $GLOBALS['test_crons'] = [];
    \Workerman\Timer::$delays = [];
    $root_scan_worker = new Worker_Process(__FILE__, 'example.test', __FILE__, $isolated_scan_fixture);
    invoke_private($root_scan_worker, 'rescan_all_jobs');
    assert_true(!in_array(175, $GLOBALS['test_switched_blogs'], true), 'Root scanning must skip proxy blog 175 because it belongs to isolated network 12');
    assert_same(1, count(\Workerman\Timer::$delays), 'A root full rescan must include the isolated tenant scanner result');
    $GLOBALS['test_sites'] = [1];
    $GLOBALS['test_site_network_ids'] = [];
    assert_true(unlink($isolated_scan_fixture), 'Isolated scan fixture must be removed');
    \Workerman\Timer::$delays = [];

    assert_true(
        str_contains($scan_script, "defined('WU_MT_LEGACY_ISOLATED_NETWORK')")
            && str_contains($scan_script, "get_sites(['number' => 0, 'fields' => 'ids'])")
            && str_contains($scan_script, 'qw_scan_current_site_jobs($scheduling_horizon)'),
        'Isolated scanner bootstrap must validate the routed network and enumerate every tenant-local site'
    );
    assert_true(
        str_contains($executor_entrypoint, 'routes_to_isolated_network($isolated_network_id)')
            && str_contains($executor_entrypoint, "defined('WU_MT_LEGACY_ISOLATED_NETWORK')"),
        'Executor bootstrap must validate isolated queue identities and the routed tenant network'
    );

    $GLOBALS['test_current_blog_id'] = 49;
    $GLOBALS['test_ms_switched'] = true;
    $GLOBALS['test_site_url'] = 'https://mapped.example.test/wp';
    $GLOBALS['test_site_domain'] = 'translate.internal.test';
    $GLOBALS['test_site_path'] = '/translations/';
    $switched_payload = new Job_Payload([
        'hook' => 'switched_site_hook',
        'args' => [],
        'timestamp' => 1710000000,
        'source' => 'action_scheduler',
        'action_id' => 46,
        'group' => 'translate',
    ]);
    assert_same(49, $switched_payload->site_id, 'Payload site ID must follow the switched blog during network scans');
    assert_same(
        'https://translate.internal.test/translations/',
        $switched_payload->site_url,
        'Payload URL must use the wp_blogs domain and path instead of a filtered siteurl during switched-site scans'
    );
    $switched_cron_payload = Job_Payload::from_cron_event((object) [
        'hook'      => 'switched_cron_hook',
        'args'      => [],
        'timestamp' => 1710000000,
        'schedule'  => 'hourly',
    ]);
    assert_same(
        'https://translate.internal.test/translations/',
        $switched_cron_payload->site_url,
        'WP-Cron factory payloads must preserve the canonical switched-site bootstrap URL'
    );
    $GLOBALS['test_mapping_table_exists'] = true;
    $GLOBALS['test_mapping_row'] = (object) [
        'domain' => 'translate.example.test',
        'secure' => '1',
    ];
    $mapped_payload = new Job_Payload([
        'hook' => 'mapped_site_hook',
        'timestamp' => 1710000000,
        'source' => 'action_scheduler',
        'action_id' => 47,
    ]);
    assert_same(
        'https://translate.example.test/translations/',
        $mapped_payload->site_url,
        'Payload URL must prefer a uniquely owned active domain mapping'
    );

    $GLOBALS['test_mapping_row'] = null;
    $GLOBALS['test_canonical_mapping_conflicts'] = 1;
    $unroutable_payload = new Job_Payload([
        'hook' => 'unroutable_site_hook',
        'timestamp' => 1710000000,
        'source' => 'action_scheduler',
        'action_id' => 48,
    ]);
    assert_same('', $unroutable_payload->site_url, 'A canonical domain mapped to another blog must fail closed');
    \Workerman\Worker::$logs = [];
    invoke_private($worker, 'schedule_timer', [$unroutable_payload]);
    invoke_private($worker, 'schedule_timer', [$unroutable_payload]);
    assert_same(1, count(\Workerman\Worker::$logs), 'An unroutable site must be skipped with one bounded diagnostic');

    $GLOBALS['test_mapping_table_exists'] = false;
    $GLOBALS['test_canonical_mapping_conflicts'] = 0;
    restore_current_blog();
    $GLOBALS['test_site_url'] = 'https://example.test';
    $GLOBALS['test_site_domain'] = 'example.test';
    $GLOBALS['test_site_path'] = '/';

    invoke_private($worker, 'schedule_timer', [new Job_Payload([
        'site_id' => 7,
        'site_url' => 'https://tenant.example.test',
        'hook' => 'due_as_hook',
        'args' => [],
        'timestamp' => time() - 10,
        'source' => 'action_scheduler',
        'action_id' => 45,
        'group' => 'checkout',
    ])]);
    assert_same(0.001, \Workerman\Timer::$delays[0] ?? null, 'Due Action Scheduler payloads must use a positive near-immediate timer delay');

    $executor = new QueueWorker\Job_Executor(1);
    ActionScheduler::$store = new Test_ActionScheduler_Store();
    ActionScheduler::$store->statuses = [
        new InvalidArgumentException('Action is not committed yet'),
        new InvalidArgumentException('Action is not committed yet'),
        ActionScheduler_Store::STATUS_PENDING,
    ];
    assert_same(
        true,
        invoke_private($executor, 'wait_for_action_scheduler_action', [44, 3, 0]),
        'An Action Scheduler row hidden by another request transaction must be retried until it becomes visible'
    );
    assert_same([44, 44, 44], ActionScheduler::$store->action_ids, 'Transaction visibility retries must check the same durable action ID');

    ActionScheduler::$store = new Test_ActionScheduler_Store();
    ActionScheduler::$store->statuses = [ActionScheduler_Store::STATUS_PENDING];
    ActionScheduler_QueueRunner::$runner = new ActionScheduler_QueueRunner();
    invoke_private($executor, 'execute_action_scheduler', [['action_id' => 45]]);
    assert_same([45], ActionScheduler_QueueRunner::$runner->processed_action_ids, 'A visible pending Action Scheduler action must run normally');

    ActionScheduler::$store = new Test_ActionScheduler_Store();
    ActionScheduler::$store->statuses = [
        new InvalidArgumentException('Missing action'),
        new InvalidArgumentException('Missing action'),
    ];
    assert_same(
        false,
        invoke_private($executor, 'wait_for_action_scheduler_action', [46, 2, 0]),
        'A still-invisible action must remain pending for the scanner instead of being passed to the queue runner'
    );

    $stale_job = [
        'hook'      => 'stale_one_shot_hook',
        'args'      => ['a' => 1],
        'timestamp' => 400,
        'schedule'  => '',
    ];
    $GLOBALS['test_crons'] = [];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_unscheduled_events'] = [];
    invoke_private($executor, 'execute_wp_cron', [$stale_job]);
    assert_same([], $GLOBALS['test_fired_actions'], 'Stale WP-Cron payloads must not fire callbacks after their cron row is gone');
    assert_same([], $GLOBALS['test_unscheduled_events'], 'Stale WP-Cron payloads must not attempt to unschedule a missing event');

    $fresh_key = md5(serialize(['a' => 1]));
    $fresh_job = [
        'hook'      => 'fresh_one_shot_hook',
        'args'      => ['a' => 1],
        'timestamp' => 500,
        'schedule'  => '',
    ];
    $GLOBALS['test_crons'] = [
        500 => [
            'fresh_one_shot_hook' => [
                $fresh_key => ['schedule' => '', 'args' => ['a' => 1]],
            ],
        ],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_unscheduled_events'] = [];
    invoke_private($executor, 'execute_wp_cron', [$fresh_job]);
    invoke_private($executor, 'execute_wp_cron', [$fresh_job]);
    assert_same([
        ['hook' => 'fresh_one_shot_hook', 'args' => ['a' => 1]],
    ], $GLOBALS['test_fired_actions'], 'A one-shot WP-Cron payload must fire once and skip duplicate stale executions');
    assert_same([], $GLOBALS['test_crons'], 'A one-shot WP-Cron payload must be removed before firing');

    $malformed_job = [
        'hook'      => 'malformed_one_shot_hook',
        'args'      => [],
        'timestamp' => 600,
        'schedule'  => '',
    ];
    $GLOBALS['test_crons'] = [
        600 => [
            'malformed_one_shot_hook' => [
                'malformed-key' => ['schedule' => '', 'args' => []],
            ],
        ],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_unscheduled_events'] = [];
    invoke_private($executor, 'execute_wp_cron', [$malformed_job]);
    invoke_private($executor, 'execute_wp_cron', [$malformed_job]);
    assert_same([
        ['hook' => 'malformed_one_shot_hook', 'args' => []],
    ], $GLOBALS['test_fired_actions'], 'Malformed-key WP-Cron payloads must be removed directly and fire once');
    assert_same([], $GLOBALS['test_crons'], 'Malformed-key WP-Cron payloads must be removed from the cron array');

    $recurring_timestamp = time() - 7200;
    // A cron dedupe may retain a later equivalent event that is not the exact
    // timestamp wp_reschedule_event() would calculate from the stale row.
    $recurring_target = $recurring_timestamp + 14400;
    $recurring_key = md5(serialize(['site_id' => 7]));
    $recurring_job = [
        'hook'      => 'duplicate_recurring_hook',
        'args'      => ['site_id' => 7],
        'timestamp' => $recurring_timestamp,
        'schedule'  => 'hourly',
    ];
    $GLOBALS['test_crons'] = [
        $recurring_timestamp => [
            'duplicate_recurring_hook' => [
                $recurring_key => ['schedule' => 'hourly', 'args' => ['site_id' => 7], 'interval' => 3600],
            ],
        ],
        $recurring_target => [
            'duplicate_recurring_hook' => [
                $recurring_key => ['schedule' => 'hourly', 'args' => ['site_id' => 7], 'interval' => 3600],
            ],
        ],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_unscheduled_events'] = [];
    $GLOBALS['test_rescheduled_events'] = [];
    $GLOBALS['test_reschedule_result'] = true;
    invoke_private($executor, 'execute_wp_cron', [$recurring_job]);
    assert_same([], $GLOBALS['test_fired_actions'], 'A stale recurring duplicate must not replay its callback');
    assert_same([], $GLOBALS['test_rescheduled_events'], 'A stale recurring duplicate must not reschedule when any later equivalent event exists');
    assert_true(!isset($GLOBALS['test_crons'][$recurring_timestamp]), 'A stale recurring duplicate must be removed after its successor is confirmed');
    assert_true(isset($GLOBALS['test_crons'][$recurring_target]['duplicate_recurring_hook'][$recurring_key]), 'The authoritative recurring successor must remain scheduled');

    $orphaned_timestamp = time() - 3600;
    $orphaned_job = [
        'hook'      => 'orphaned_recurring_hook',
        'args'      => ['site_id' => 7],
        'timestamp' => $orphaned_timestamp,
        'schedule'  => 'unavailable_cleanup',
    ];
    $GLOBALS['test_schedules'] = [];
    $GLOBALS['test_crons'] = [
        $orphaned_timestamp => [
            'orphaned_recurring_hook' => [
                $recurring_key => ['schedule' => 'unavailable_cleanup', 'args' => ['site_id' => 7], 'interval' => 3600],
            ],
        ],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_rescheduled_events'] = [];
    $GLOBALS['test_reschedule_result'] = true;
    try {
        invoke_private($executor, 'execute_wp_cron', [$orphaned_job]);
        throw new RuntimeException('Unavailable recurrences must report their removal');
    } catch (RuntimeException $exception) {
        assert_true(str_contains($exception->getMessage(), 'Removed orphaned cron event'), 'Unavailable recurrences must be reported as orphaned cleanup');
    }
    assert_same([], $GLOBALS['test_fired_actions'], 'An orphaned recurring event must not fire its callback');
    assert_same(1, count($GLOBALS['test_rescheduled_events']), 'An orphaned recurring event must identify the invalid schedule through WordPress');
    assert_same([], $GLOBALS['test_crons'], 'An orphaned recurring event must be removed instead of retrying indefinitely');

    $GLOBALS['test_schedules'] = [
        'unavailable_cleanup' => ['interval' => 3600],
        'hourly'               => ['interval' => 3600],
    ];
    $GLOBALS['test_crons'] = [
        $orphaned_timestamp => [
            'orphaned_recurring_hook' => [
                $recurring_key => ['schedule' => 'unavailable_cleanup', 'args' => ['site_id' => 7], 'interval' => 3600],
            ],
        ],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_rescheduled_events'] = [];
    invoke_private($executor, 'execute_wp_cron', [$orphaned_job]);
    assert_same([
        ['hook' => 'orphaned_recurring_hook', 'args' => ['site_id' => 7]],
    ], $GLOBALS['test_fired_actions'], 'A recurring event must resume normal execution after its provider returns');
    assert_same([], $GLOBALS['test_crons'], 'A recovered recurring event must remove its executed row after rescheduling');

    $failure_timestamp = time() - 1800;
    $failure_job = [
        'hook'      => 'failing_recurring_hook',
        'args'      => [],
        'timestamp' => $failure_timestamp,
        'schedule'  => 'hourly',
    ];
    $GLOBALS['test_crons'] = [
        $failure_timestamp => [
            'failing_recurring_hook' => [
                md5(serialize([])) => ['schedule' => 'hourly', 'args' => [], 'interval' => 3600],
            ],
        ],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_reschedule_result'] = false;
    try {
        invoke_private($executor, 'execute_wp_cron', [$failure_job]);
        throw new RuntimeException('Non-invalid-schedule rescheduling failures must remain visible');
    } catch (RuntimeException $exception) {
        assert_true(str_contains($exception->getMessage(), 'Failed to reschedule cron event'), 'Non-invalid-schedule rescheduling failures must remain visible');
    }
    assert_same([], $GLOBALS['test_fired_actions'], 'A recurring event with a transient rescheduling failure must not fire its callback');
    assert_true(isset($GLOBALS['test_crons'][$failure_timestamp]['failing_recurring_hook']), 'A recurring event with a transient rescheduling failure must remain available for retry');
    $GLOBALS['test_reschedule_result'] = true;

    $malformed_successor_key = 'malformed-successor-key';
    $GLOBALS['test_crons'] = [
        $recurring_timestamp => [
            'duplicate_recurring_malformed_successor_hook' => [
                $recurring_key => ['schedule' => 'hourly', 'args' => ['site_id' => 7], 'interval' => 3600],
            ],
        ],
        $recurring_target => [
            'duplicate_recurring_malformed_successor_hook' => [
                $malformed_successor_key => ['schedule' => 'hourly', 'args' => ['site_id' => 7], 'interval' => 3600],
            ],
        ],
    ];
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_rescheduled_events'] = [];
    invoke_private($executor, 'execute_wp_cron', [[
        'hook'      => 'duplicate_recurring_malformed_successor_hook',
        'args'      => ['site_id' => 7],
        'timestamp' => $recurring_timestamp,
        'schedule'  => 'hourly',
    ]]);
    assert_same([], $GLOBALS['test_fired_actions'], 'A stale recurring duplicate must not fire while normalizing a malformed successor key');
    assert_same([], $GLOBALS['test_rescheduled_events'], 'A stale recurring duplicate must not reschedule when its successor key is malformed');
    assert_true(!isset($GLOBALS['test_crons'][$recurring_timestamp]), 'A stale recurring duplicate must be removed after its malformed successor is normalized');
    assert_true(isset($GLOBALS['test_crons'][$recurring_target]['duplicate_recurring_malformed_successor_hook'][$recurring_key]), 'A malformed recurring successor key must be repaired to WordPress canonical form');
    assert_true(!isset($GLOBALS['test_crons'][$recurring_target]['duplicate_recurring_malformed_successor_hook'][$malformed_successor_key]), 'The malformed recurring successor key must be removed after repair');

    putenv('QUEUE_WORKER_SCHEDULING_HORIZON');
    putenv('QUEUE_WORKER_RESCAN_INTERVAL');
    assert_same(3600, Config::scheduling_horizon(), 'Scheduling horizon must default to one hour');
    putenv('QUEUE_WORKER_SCHEDULING_HORIZON=12');
    assert_same(60, Config::scheduling_horizon(), 'Scheduling horizon must not be shorter than the rescan interval');
    putenv('QUEUE_WORKER_RESCAN_INTERVAL=10');
    assert_same(12, Config::scheduling_horizon(), 'Scheduling horizon must remain configurable above the rescan interval');
    putenv('QUEUE_WORKER_SCAN_TIMEOUT');
    assert_same(300, Config::scan_timeout(), 'Full-network scan timeout must default to five minutes');
    putenv('QUEUE_WORKER_SCAN_TIMEOUT=0');
    assert_same(1, Config::scan_timeout(), 'Full-network scan timeout must be clamped to at least one second');
    putenv('QUEUE_WORKER_SCAN_TIMEOUT');
    putenv('QUEUE_WORKER_RESCAN_INTERVAL');
    putenv('QUEUE_WORKER_SCHEDULING_HORIZON=2147483647');

    putenv('QUEUE_WORKER_AS_RESCAN_INTERVAL');
    assert_same(5, Config::action_scheduler_rescan_interval(), 'AS rescan interval must default to five seconds');
    putenv('QUEUE_WORKER_AS_RESCAN_INTERVAL=12');
    assert_same(12, Config::action_scheduler_rescan_interval(), 'AS rescan interval must be configurable');
    putenv('QUEUE_WORKER_AS_RESCAN_INTERVAL=0');
    assert_same(1, Config::action_scheduler_rescan_interval(), 'AS rescan interval must be clamped to at least one second');

    assert_true(Cron_Event_Filter::should_bypass('wp_update_plugins'), 'Bypass hook must be skipped by shared cron filter');
    assert_true(Cron_Event_Filter::should_bypass('action_scheduler_run_queue'), 'Action Scheduler queue runner must be skipped by shared cron filter');
    assert_true(!Cron_Event_Filter::should_bypass('custom_hook'), 'Custom hooks must not be bypassed by shared cron filter');
    putenv('QUEUE_WORKER_BYPASS_CRON_HOOKS=custom_hook, extra_hook');
    assert_true(Cron_Event_Filter::should_bypass('wp_update_plugins'), 'Default bypass hooks must remain active when custom hooks are configured');
    assert_true(Cron_Event_Filter::should_bypass('custom_hook'), 'Configured bypass hook must be skipped by shared cron filter');
    assert_true(Cron_Event_Filter::should_bypass('extra_hook'), 'Comma-separated bypass hooks must be normalized');
    putenv('QUEUE_WORKER_BYPASS_CRON_HOOKS');
    putenv('QUEUE_WORKER_MANAGED_CRON_HOOKS=wp_version_check, wp_update_plugins, wp_update_themes');
    assert_true(!Cron_Event_Filter::should_bypass('wp_version_check'), 'Managed core update hook must not be bypassed');
    assert_true(!Cron_Event_Filter::should_bypass('wp_update_plugins'), 'Managed plugin update hook must not be bypassed');
    assert_true(!Cron_Event_Filter::should_bypass('wp_update_themes'), 'Managed theme update hook must not be bypassed');
    assert_true(Cron_Event_Filter::should_bypass('action_scheduler_run_queue'), 'Unmanaged default bypass hooks must remain active');
    putenv('QUEUE_WORKER_MANAGED_CRON_HOOKS');

    Cron_Interceptor::register();
    assert_same([], $GLOBALS['test_actions'], 'Cron interceptor must register schedule_event as a filter, not an action');
    assert_same('schedule_event', $GLOBALS['test_filters'][0]['hook'] ?? '', 'Cron interceptor must hook schedule_event');
    assert_same([Cron_Interceptor::class, 'on_schedule_event'], $GLOBALS['test_filters'][0]['callback'] ?? null, 'Cron interceptor must register its event callback');
    $event = (object) ['hook' => 'wp_update_plugins'];
    assert_same($event, Cron_Interceptor::on_schedule_event($event), 'Cron interceptor filter must return the event unchanged');
    assert_same(null, Cron_Interceptor::on_schedule_event(null), 'Cron interceptor filter must preserve invalid event values');

    assert_same(
        Cron_Event_Filter::signature('custom_hook', ['schedule' => 'hourly', 'args' => ['a' => 1]], 100),
        Cron_Event_Filter::signature('custom_hook', ['schedule' => 'hourly', 'args' => ['a' => 1]], 200),
        'Recurring cron duplicates must collapse across timestamps for the same hook, schedule, and args'
    );

    $GLOBALS['test_crons'] = [
        100 => [
            'wp_update_plugins' => [
                'ignored' => ['schedule' => 'hourly', 'args' => []],
            ],
            'custom_hook' => [
                'first' => ['schedule' => 'hourly', 'args' => ['a' => 1]],
            ],
        ],
        200 => [
            'custom_hook' => [
                'duplicate' => ['schedule' => 'hourly', 'args' => ['a' => 1]],
            ],
        ],
    ];
    $scan_worker = new Worker_Process(__FILE__, 'example.test', __FILE__);
    invoke_private($scan_worker, 'rescan_all_jobs');
    $pending = private_property($scan_worker, 'pending_timers');
    assert_same(1, count($pending), 'Worker scan must skip bypass hooks and collapse duplicate cron signatures');
    $scheduled_payload = private_property($scan_worker, 'pending_timers') ? array_key_first($pending) : '';
    assert_true(str_contains($scheduled_payload, 'custom_hook'), 'Worker scan must schedule the non-bypassed hook');

    $GLOBALS['test_crons'] = [
        300 => [
            'recurring_hook' => [
                'first' => ['schedule' => 'hourly', 'args' => ['a' => 1]],
            ],
            'one_shot_hook' => [
                'one' => ['schedule' => '', 'args' => ['a' => 1]],
            ],
        ],
        100 => [
            'recurring_hook' => [
                'earliest' => ['schedule' => 'hourly', 'args' => ['a' => 1]],
            ],
        ],
        200 => [
            'recurring_hook' => [
                'middle' => ['schedule' => 'hourly', 'args' => ['a' => 1]],
                'different_args' => ['schedule' => 'hourly', 'args' => ['a' => 2]],
            ],
            'one_shot_hook' => [
                'two' => ['schedule' => '', 'args' => ['a' => 1]],
            ],
        ],
    ];
    $duplicate_crons = $GLOBALS['test_crons'];
    $cli = new QueueWorker\CLI_Commands();
    $dedupe_doc = (new ReflectionMethod($cli, 'dedupe_cron'))->getDocComment();
    assert_true(is_string($dedupe_doc), 'Dedupe command must have a WP-CLI docblock');
    assert_true(str_contains($dedupe_doc, '[--dry-run]'), 'Dedupe dry-run flag must use optional WP-CLI synopsis syntax');
    assert_true(str_contains($dedupe_doc, '[--apply]'), 'Dedupe apply flag must use optional WP-CLI synopsis syntax');
    assert_true(!preg_match('/^\s*\*\s+--(?:dry-run|apply)\s*$/m', $dedupe_doc), 'Dedupe flags must not use bare WP-CLI synopsis syntax');

    $GLOBALS['test_crons'] = [
        100 => [
            'single_recurring_hook' => [
                'only' => ['schedule' => 'hourly', 'args' => []],
            ],
        ],
    ];
    assert_same(
        ['groups' => 0, 'retained' => 0, 'removed' => 0, 'rows' => []],
        invoke_private($cli, 'cron_dedupe_site_report', [1, true]),
        'Apply must not write unchanged cron arrays when no duplicates exist'
    );

    $GLOBALS['test_crons'] = $duplicate_crons;
    $canonical_recurring_key = md5(serialize(['a' => 1]));
    $groups = invoke_private($cli, 'cron_duplicate_groups', [$GLOBALS['test_crons']]);
    $duplicate_groups = array_values(array_filter($groups, static function (array $group): bool {
        return count($group['events']) > 1;
    }));
    assert_same(1, count($duplicate_groups), 'Only recurring events with identical hook, schedule, and args should be duplicate groups');
    assert_same('recurring_hook', $duplicate_groups[0]['hook'], 'Duplicate group must preserve the hook');
    assert_same(100, $duplicate_groups[0]['events'][0]['timestamp'], 'Earliest duplicate timestamp must sort first');
    assert_same(200, $duplicate_groups[0]['events'][1]['timestamp'], 'Middle duplicate timestamps must remain sorted');
    assert_same(300, $duplicate_groups[0]['events'][2]['timestamp'], 'Latest duplicate timestamp must be sorted last');

    $GLOBALS['test_unscheduled_events'] = [];
    $dry_report = invoke_private($cli, 'cron_dedupe_site_report', [1, false]);
    assert_same(1, $dry_report['groups'], 'Dry-run must report one duplicate recurring group');
    assert_same(1, $dry_report['retained'], 'Dry-run must retain one event per duplicate group');
    assert_same(2, $dry_report['removed'], 'Dry-run must count all non-retained duplicate events as removable');
    assert_same(300, $dry_report['rows'][0]['retained_timestamp'], 'When all duplicate events are overdue, dedupe must retain the newest occurrence');
    assert_same([], $GLOBALS['test_unscheduled_events'], 'Dry-run must not unschedule events');

    $apply_report = invoke_private($cli, 'cron_dedupe_site_report', [1, true]);
    assert_same(2, $apply_report['removed'], 'Apply must count removed duplicate events');
    assert_true(
        !isset($GLOBALS['test_crons'][100]['recurring_hook']) && !isset($GLOBALS['test_crons'][200]['recurring_hook']['middle']),
        'Apply must remove malformed-key duplicate rows directly'
    );
    assert_true(isset($GLOBALS['test_crons'][300]['recurring_hook'][$canonical_recurring_key]), 'Apply must retain the newest overdue recurring event under its canonical WordPress key');
    assert_true(!isset($GLOBALS['test_crons'][300]['recurring_hook']['first']), 'Apply must remove the retained event malformed key after canonicalizing it');

    assert_same(
        2,
        invoke_private($cli, 'cron_dedupe_retained_event_index', [[
            ['timestamp' => time() - 3600],
            ['timestamp' => time() + 300],
            ['timestamp' => time() + 3600],
        ]]),
        'Dedupe must retain the newest recurring event when future duplicates exist'
    );

    $GLOBALS['wpdb']->queries = [];
    $GLOBALS['wpdb']->query_results = [10000, 10000, 23];
    assert_same(20023, Job_Log::cleanup(7), 'Job log cleanup must report rows deleted across every batch');
    assert_same(3, count($GLOBALS['wpdb']->queries), 'Job log cleanup must continue until a partial batch is deleted');
    foreach ($GLOBALS['wpdb']->queries as $cleanup_query) {
        assert_true(str_contains($cleanup_query, 'ORDER BY completed_at ASC'), 'Job log cleanup must delete the oldest rows first');
        assert_true(str_contains($cleanup_query, 'LIMIT 10000'), 'Job log cleanup must bound each delete statement');
    }

    assert_same(false, Socket_Client::filter_wp_cron_status_override(false), 'Cron health override must preserve a prior non-null decision');
    putenv('QUEUE_WORKER_SOCKET_PATH=' . sys_get_temp_dir() . '/qw-missing-socket-' . getmypid());
    assert_same(null, Socket_Client::filter_wp_cron_status_override(null), 'Cron health override must retain the null sentinel when no worker is available');
    if (function_exists('pcntl_fork')) {
        $status_socket = sys_get_temp_dir() . '/qw-status-' . getmypid() . '.sock';
        @unlink($status_socket);
        $status_server = stream_socket_server('unix://' . $status_socket, $status_errno, $status_error);
        assert_true(is_resource($status_server), 'Cron health status fixture must create a Unix socket: ' . $status_error);
        $status_pid = pcntl_fork();
        assert_true($status_pid >= 0, 'Cron health status fixture must fork');
        if ($status_pid === 0) {
            $status_connection = stream_socket_accept($status_server, 2);
            if (is_resource($status_connection)) {
                fgets($status_connection);
                fwrite($status_connection, json_encode(['ready' => true]) . "\n");
                fclose($status_connection);
            }
            fclose($status_server);
            exit(0);
        }
        fclose($status_server);
        putenv('QUEUE_WORKER_SOCKET_PATH=' . $status_socket);
        assert_same(true, Socket_Client::filter_wp_cron_status_override(null), 'A ready Socket_Client worker must report intentional external cron as healthy');
        pcntl_waitpid($status_pid, $status_result);
        assert_true(unlink($status_socket), 'Cron health status fixture socket must be removed');
    }
    putenv('QUEUE_WORKER_SOCKET_PATH');

    define('WU_MT_LEGACY_ISOLATED_NETWORK', 49);
    $GLOBALS['test_current_blog_id'] = 2;
    $live_isolated_payload = Job_Payload::from_cron_event((object) [
        'hook'      => 'live_isolated_hook',
        'args'      => [],
        'timestamp' => 2147483647,
        'schedule'  => '',
    ]);
    assert_same(Job_Payload::isolated_site_id(49, 2), $live_isolated_payload->site_id, 'Live request interception must use the collision-safe isolated queue identity');
    assert_same(49, $live_isolated_payload->isolated_network_id, 'Live request payloads must carry their isolated network route');
    assert_same(2, $live_isolated_payload->local_site_id, 'Live request payloads must carry their tenant-local blog route');

    echo "Regression tests passed.\n";
}
