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
}

namespace {
    use QueueWorker\Config;
    use QueueWorker\Cron_Event_Filter;
    use QueueWorker\Cron_Interceptor;
    use QueueWorker\Job_Payload;
    use QueueWorker\Worker_Process;

    $_SERVER['HTTP_HOST'] = 'example.test';

    $GLOBALS['test_crons'] = [];
    $GLOBALS['test_current_blog_id'] = 1;
    $GLOBALS['test_filters'] = [];
    $GLOBALS['test_actions'] = [];
    $GLOBALS['test_switched_blogs'] = [];
    $GLOBALS['test_unscheduled_events'] = [];
    $GLOBALS['test_rescheduled_events'] = [];
    $GLOBALS['test_reschedule_result'] = true;
    $GLOBALS['test_fired_actions'] = [];
    $GLOBALS['test_cache_deletes'] = [];
    $GLOBALS['test_ms_switched'] = false;

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
            $key = null;

            foreach ($GLOBALS['test_crons'][$timestamp][$hook] ?? [] as $event_key => $event) {
                if (($event['args'] ?? []) === $args) {
                    $key = $event_key;
                    break;
                }
            }

            if ($key === null) {
                return false;
            }
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

        return $GLOBALS['test_reschedule_result'];
    }

    function is_wp_error($thing): bool
    {
        unset($thing);

        return false;
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
    require_once __DIR__ . '/../src/class-job-payload.php';
    require_once __DIR__ . '/../src/class-job-executor.php';
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
    $GLOBALS['test_current_blog_id'] = 49;
    $GLOBALS['test_ms_switched'] = true;
    $switched_payload = new Job_Payload([
        'hook' => 'switched_site_hook',
        'args' => [],
        'timestamp' => 1710000000,
        'source' => 'action_scheduler',
        'action_id' => 46,
        'group' => 'translate',
    ]);
    assert_same(49, $switched_payload->site_id, 'Payload site ID must follow the switched blog during network scans');
    restore_current_blog();

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
    assert_same([
        ['timestamp' => 500, 'hook' => 'fresh_one_shot_hook', 'args' => ['a' => 1]],
    ], $GLOBALS['test_unscheduled_events'], 'A one-shot WP-Cron payload must be unscheduled before firing');

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
    $recurring_target = $recurring_timestamp + 10800;
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
    assert_same([], $GLOBALS['test_rescheduled_events'], 'A stale recurring duplicate must not reschedule an existing successor');
    assert_same([
        ['timestamp' => $recurring_timestamp, 'hook' => 'duplicate_recurring_hook', 'args' => ['site_id' => 7]],
    ], $GLOBALS['test_unscheduled_events'], 'A stale recurring duplicate must be removed after its successor is confirmed');
    assert_true(isset($GLOBALS['test_crons'][$recurring_target]['duplicate_recurring_hook'][$recurring_key]), 'The authoritative recurring successor must remain scheduled');

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
    $cli = new QueueWorker\CLI_Commands();
    $dedupe_doc = (new ReflectionMethod($cli, 'dedupe_cron'))->getDocComment();
    assert_true(is_string($dedupe_doc), 'Dedupe command must have a WP-CLI docblock');
    assert_true(str_contains($dedupe_doc, '[--dry-run]'), 'Dedupe dry-run flag must use optional WP-CLI synopsis syntax');
    assert_true(str_contains($dedupe_doc, '[--apply]'), 'Dedupe apply flag must use optional WP-CLI synopsis syntax');
    assert_true(!preg_match('/^\s*\*\s+--(?:dry-run|apply)\s*$/m', $dedupe_doc), 'Dedupe flags must not use bare WP-CLI synopsis syntax');

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
    assert_same([
        ['timestamp' => 100, 'hook' => 'recurring_hook', 'args' => ['a' => 1]],
        ['timestamp' => 200, 'hook' => 'recurring_hook', 'args' => ['a' => 1]],
    ], $GLOBALS['test_unscheduled_events'], 'Apply must retain only the newest overdue recurring event');

    assert_same(
        1,
        invoke_private($cli, 'cron_dedupe_retained_event_index', [[
            ['timestamp' => time() - 3600],
            ['timestamp' => time() + 300],
            ['timestamp' => time() + 3600],
        ]]),
        'Dedupe must retain the nearest future recurring event when one exists'
    );

    echo "Regression tests passed.\n";
}
