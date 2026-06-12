<?php

namespace Workerman {
    class Worker
    {
        public int $id = 0;

        public static function log(string $message): void
        {
        }

        public static function stopAll(): void
        {
        }
    }

    class Timer
    {
        public static int $next_id = 1;

        public static function add($delay, callable $callback, array $args = [], bool $persistent = true): int
        {
            return self::$next_id++;
        }
    }
}

namespace {
    use QueueWorker\Config;
    use QueueWorker\Cron_Event_Filter;
    use QueueWorker\Job_Payload;
    use QueueWorker\Worker_Process;

    $_SERVER['HTTP_HOST'] = 'example.test';

    $GLOBALS['test_crons'] = [];
    $GLOBALS['test_current_blog_id'] = 1;
    $GLOBALS['test_switched_blogs'] = [];

    class Test_WPDB
    {
        public string $prefix = 'wp_';
        public string $base_prefix = 'wp_';

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
            return null;
        }

        public function query(string $query): int
        {
            return 1;
        }
    }

    $GLOBALS['wpdb'] = new Test_WPDB();

    function get_site_url(): string
    {
        return 'https://example.test';
    }

    function get_current_blog_id(): int
    {
        return (int) $GLOBALS['test_current_blog_id'];
    }

    function wp_get_schedules(): array
    {
        return [
            'hourly' => ['interval' => 3600],
        ];
    }

    function get_sites(array $args): array
    {
        return [1];
    }

    function switch_to_blog(int $site_id): void
    {
        $GLOBALS['test_current_blog_id'] = $site_id;
        $GLOBALS['test_switched_blogs'][] = $site_id;
    }

    function restore_current_blog(): void
    {
        $GLOBALS['test_current_blog_id'] = 1;
    }

    function wp_cache_delete(string $key, string $group): void
    {
    }

    function _get_cron_array(): array
    {
        return $GLOBALS['test_crons'];
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

    function invoke_private(object $object, string $method, array $args = [])
    {
        $reflection = new ReflectionMethod($object, $method);
        $reflection->setAccessible(true);

        return $reflection->invokeArgs($object, $args);
    }

    require_once __DIR__ . '/../src/class-config.php';
    require_once __DIR__ . '/../src/class-cron-event-filter.php';
    require_once __DIR__ . '/../src/class-job-payload.php';
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
    assert_true(str_contains($payload->tracking_key(), ':checkout:'), 'Action Scheduler group must be part of the tracking key');

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

    putenv('QUEUE_WORKER_AS_RESCAN_INTERVAL');
    assert_same(5, Config::action_scheduler_rescan_interval(), 'AS rescan interval must default to five seconds');
    putenv('QUEUE_WORKER_AS_RESCAN_INTERVAL=12');
    assert_same(12, Config::action_scheduler_rescan_interval(), 'AS rescan interval must be configurable');
    putenv('QUEUE_WORKER_AS_RESCAN_INTERVAL=0');
    assert_same(1, Config::action_scheduler_rescan_interval(), 'AS rescan interval must be clamped to at least one second');

    assert_true(Cron_Event_Filter::should_bypass('wp_update_plugins'), 'Bypass hook must be skipped by shared cron filter');
    assert_true(Cron_Event_Filter::should_bypass('action_scheduler_run_queue'), 'Action Scheduler queue runner must be skipped by shared cron filter');
    assert_true(!Cron_Event_Filter::should_bypass('custom_hook'), 'Custom hooks must not be bypassed by shared cron filter');
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

    echo "Regression tests passed.\n";
}
