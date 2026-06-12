<?php

namespace QueueWorker;

use WP_CLI;

class CLI_Commands
{
    /**
     * Show queue worker status.
     *
     * ## EXAMPLES
     *
     *     wp queue status
     *
     * @subcommand status
     */
    public function status($args, $assoc_args): void
    {
        if (!Socket_Client::is_worker_running()) {
            WP_CLI::warning('Queue worker is not running (no socket file).');
            return;
        }

        $data = Socket_Client::send_command('status');
        if (!$data) {
            $error = Socket_Client::get_last_error() ?: 'unknown_error';
            WP_CLI::warning(sprintf('Worker status request failed: %s.', $error));
            return;
        }

        WP_CLI::success('Queue worker is running.');
        WP_CLI::log(sprintf('  PID:            %d', $data['pid'] ?? 0));
        WP_CLI::log(sprintf('  Uptime:         %s', $data['uptime'] ?? 'unknown'));
        WP_CLI::log(sprintf('  Pending timers: %d', $data['pending_timers'] ?? 0));
        WP_CLI::log(sprintf('  Pending cron:   %d', $data['pending_cron_batches'] ?? 0));
        WP_CLI::log(sprintf('  Pending AS:     %d', $data['pending_as_batches'] ?? 0));
        WP_CLI::log(sprintf('  Running jobs:   %d', $data['running_jobs'] ?? 0));
        WP_CLI::log(sprintf('  Running cron:   %d', $data['running_cron_jobs'] ?? 0));
        WP_CLI::log(sprintf('  Running AS:     %d', $data['running_as_jobs'] ?? 0));
        WP_CLI::log(sprintf('  Memory:         %s', $data['memory'] ?? 'unknown'));

        if (!empty($data['rescan']) && is_array($data['rescan'])) {
            WP_CLI::log(sprintf(
                '  Rescan:         %s (last duration: %ds)',
                !empty($data['rescan']['in_progress']) ? 'in progress' : 'idle',
                (int) ($data['rescan']['last_duration'] ?? 0)
            ));
        }

        if (!empty($data['pending_by_source']) && is_array($data['pending_by_source'])) {
            WP_CLI::log('  Pending timers by source:');
            foreach ($data['pending_by_source'] as $source => $count) {
                WP_CLI::log(sprintf('    - %s: %d', $source, $count));
            }
        }

        if (!empty($data['pending_by_hook']) && is_array($data['pending_by_hook'])) {
            WP_CLI::log('  Pending timers by hook:');
            $hooks = array_slice($data['pending_by_hook'], 0, 10, true);
            foreach ($hooks as $hook => $count) {
                WP_CLI::log(sprintf('    - %s: %d', $hook, $count));
            }

            $remaining = count($data['pending_by_hook']) - count($hooks);
            if ($remaining > 0) {
                WP_CLI::log(sprintf('    - ... %d more hooks', $remaining));
            }
        }

        if (!empty($data['cron_lanes']) && is_array($data['cron_lanes'])) {
            WP_CLI::log('  Cron lanes:');
            foreach ($data['cron_lanes'] as $lane => $details) {
                WP_CLI::log(sprintf(
                    '    - %s: pending %d, running %d, max %d, batch %d',
                    $lane,
                    (int) ($details['pending'] ?? 0),
                    (int) ($details['running'] ?? 0),
                    (int) ($details['max'] ?? 0),
                    (int) ($details['batch'] ?? 0)
                ));
            }
        }

        if (!empty($data['as_lanes']) && is_array($data['as_lanes'])) {
            WP_CLI::log('  AS lanes:');
            foreach ($data['as_lanes'] as $lane => $details) {
                WP_CLI::log(sprintf(
                    '    - %s: pending %d, running %d, max %d, batch %d',
                    $lane,
                    (int) ($details['pending'] ?? 0),
                    (int) ($details['running'] ?? 0),
                    (int) ($details['max'] ?? 0),
                    (int) ($details['batch'] ?? 0)
                ));
            }
        }

        if (empty($data['cron_lanes']) && !empty($data['pending_cron_lanes'])) {
            WP_CLI::log('  Pending cron lanes:');
            foreach ($data['pending_cron_lanes'] as $lane => $count) {
                WP_CLI::log(sprintf('    - %s: %d', $lane, $count));
            }
        }

        if (empty($data['as_lanes']) && !empty($data['pending_as_lanes'])) {
            WP_CLI::log('  Pending AS lanes:');
            foreach ($data['pending_as_lanes'] as $lane => $count) {
                WP_CLI::log(sprintf('    - %s: %d', $lane, $count));
            }
        }

        if (empty($data['cron_lanes']) && !empty($data['running_cron_lanes'])) {
            WP_CLI::log('  Running cron lanes:');
            foreach ($data['running_cron_lanes'] as $lane => $count) {
                WP_CLI::log(sprintf('    - %s: %d', $lane, $count));
            }
        }

        if (empty($data['as_lanes']) && !empty($data['running_as_lanes'])) {
            WP_CLI::log('  Running AS lanes:');
            foreach ($data['running_as_lanes'] as $lane => $count) {
                WP_CLI::log(sprintf('    - %s: %d', $lane, $count));
            }
        }

        if (!empty($data['running_details'])) {
            WP_CLI::log('  Currently executing:');
            foreach ($data['running_details'] as $detail) {
                WP_CLI::log(sprintf(
                    '    - %s site %d: %s (%d jobs, %ds elapsed)',
                    $detail['lane'] ?? 'wp_cron',
                    $detail['site_id'],
                    $detail['hook'],
                    $detail['count'],
                    $detail['elapsed']
                ));
            }
        }
    }

    /**
     * Force rescan of all pending jobs and send to worker.
     *
     * ## EXAMPLES
     *
     *     wp queue populate
     *
     * @subcommand populate
     */
    public function populate($args, $assoc_args): void
    {
        if (!Socket_Client::is_worker_running()) {
            WP_CLI::error('Queue worker is not running.');
        }

        $count = 0;

        // Scan WP Cron events across all sites
        $sites = get_sites(['number' => 0, 'fields' => 'ids']);
        foreach ($sites as $site_id) {
            switch_to_blog($site_id);

            // Scan Action Scheduler pending actions only when this switched
            // site has its own scheduler tables. Some multisite blogs have no
            // per-site Action Scheduler tables, and probing them directly
            // emits database errors.
            if (function_exists('as_get_scheduled_actions') && $this->action_scheduler_tables_exist()) {
                $actions = as_get_scheduled_actions([
                    'status'   => \ActionScheduler_Store::STATUS_PENDING,
                    'per_page' => 500,
                ]);
                foreach ($actions as $action_id => $action) {
                    $payload = Job_Payload::from_as_action($action_id);
                    if ($payload && Socket_Client::notify($payload)) {
                        $count++;
                    }
                }
            }

            $crons = _get_cron_array();
            if (is_array($crons)) {
                $seen_cron_signatures = [];
                foreach ($crons as $timestamp => $hooks) {
                    foreach ($hooks as $hook => $events) {
                        if (Cron_Event_Filter::should_bypass($hook)) {
                            continue;
                        }

                        foreach ($events as $key => $event) {
                            $signature = Cron_Event_Filter::signature($hook, $event, (int) $timestamp);
                            if (isset($seen_cron_signatures[$signature])) {
                                continue;
                            }
                            $seen_cron_signatures[$signature] = true;

                            $event_obj = (object) array_merge($event, [
                                'hook'      => $hook,
                                'timestamp' => $timestamp,
                            ]);
                            $payload = Job_Payload::from_cron_event($event_obj);
                            if (Socket_Client::notify($payload)) {
                                $count++;
                            }
                        }
                    }
                }
            }

            restore_current_blog();
        }

        WP_CLI::success("Sent $count jobs to the queue worker.");
    }

    /**
     * Report or remove duplicate persisted WP-Cron events.
     *
     * Duplicate recurring events can accumulate when long-running event-loop
     * execution, wp_reschedule_event(), backlog catch-up, and plugin bootstrap
     * scheduling all touch the same hook. This command groups events by site,
     * hook, schedule, and args; keeps the earliest timestamp; and optionally
     * removes later timestamps for identical recurring/event signatures.
     *
     * ## OPTIONS
     *
     * --dry-run
     * : Report duplicate groups without modifying cron options.
     *
     * --apply
     * : Remove later duplicate events with wp_unschedule_event().
     *
     * ## EXAMPLES
     *
     *     wp queue dedupe-cron --dry-run
     *     wp queue dedupe-cron --apply
     *
     * @subcommand dedupe-cron
     */
    public function dedupe_cron($args, $assoc_args): void
    {
        $dry_run = (bool) ($assoc_args['dry-run'] ?? false);
        $apply   = (bool) ($assoc_args['apply'] ?? false);

        if ($dry_run === $apply) {
            WP_CLI::error('Specify exactly one of --dry-run or --apply.');
        }

        $totals = [
            'sites'    => 0,
            'groups'   => 0,
            'retained' => 0,
            'removed'  => 0,
        ];
        $rows = [];

        foreach ($this->cron_dedupe_site_ids() as $site_id) {
            $site_id = (int) $site_id;
            $totals['sites']++;

            if (is_multisite()) {
                switch_to_blog($site_id);
            }

            try {
                $report = $this->cron_dedupe_site_report($site_id, $apply);
            } finally {
                if (is_multisite()) {
                    restore_current_blog();
                }
            }

            $totals['groups']   += $report['groups'];
            $totals['retained'] += $report['retained'];
            $totals['removed']  += $report['removed'];
            $rows = array_merge($rows, $report['rows']);
        }

        WP_CLI::log($apply ? 'Duplicate WP-Cron cleanup applied.' : 'Duplicate WP-Cron dry-run report. No changes were made.');
        WP_CLI::log('Grouping key: site_id + hook + schedule + args; earliest timestamp retained.');

        if (!empty($rows)) {
            \WP_CLI\Utils\format_items('table', $rows, ['site_id', 'hook', 'schedule', 'duplicates', 'retained_timestamp', 'removed_timestamps']);
        } else {
            WP_CLI::log('No duplicate WP-Cron events found.');
        }

        WP_CLI::log(sprintf('Sites scanned: %d', $totals['sites']));
        WP_CLI::log(sprintf('Duplicate groups: %d', $totals['groups']));
        WP_CLI::log(sprintf('Retained events: %d', $totals['retained']));
        WP_CLI::log(sprintf('%s events: %d', $apply ? 'Removed' : 'Would remove', $totals['removed']));

        WP_CLI::success($apply ? 'Cron dedupe complete.' : 'Cron dedupe dry-run complete.');
    }

    /**
     * @return list<int>
     */
    private function cron_dedupe_site_ids(): array
    {
        if (!is_multisite()) {
            return [get_current_blog_id()];
        }

        return array_map('intval', get_sites(['number' => 0, 'fields' => 'ids']));
    }

    /**
     * @return array{groups: int, retained: int, removed: int, rows: list<array<string, int|string>>}
     */
    private function cron_dedupe_site_report(int $site_id, bool $apply): array
    {
        $crons = _get_cron_array();
        if (!is_array($crons)) {
            return ['groups' => 0, 'retained' => 0, 'removed' => 0, 'rows' => []];
        }

        $groups = $this->cron_duplicate_groups($crons);
        $report = ['groups' => 0, 'retained' => 0, 'removed' => 0, 'rows' => []];

        foreach ($groups as $group) {
            if (count($group['events']) < 2) {
                continue;
            }

            $duplicates = array_slice($group['events'], 1);
            $removed_timestamps = [];

            foreach ($duplicates as $event) {
                $removed_timestamps[] = (int) $event['timestamp'];

                if ($apply) {
                    wp_unschedule_event((int) $event['timestamp'], $group['hook'], $group['args']);
                }
            }

            $duplicate_count = count($duplicates);
            $report['groups']++;
            $report['retained']++;
            $report['removed'] += $duplicate_count;
            $report['rows'][] = [
                'site_id'            => $site_id,
                'hook'               => $group['hook'],
                'schedule'           => $group['schedule'] !== '' ? $group['schedule'] : '(one-shot)',
                'duplicates'         => $duplicate_count,
                'retained_timestamp' => (int) $group['events'][0]['timestamp'],
                'removed_timestamps' => implode(',', array_map('strval', $removed_timestamps)),
            ];
        }

        return $report;
    }

    /**
     * @return array<string, array{hook: string, schedule: string, args: array, events: list<array{timestamp: int}>}>
     */
    private function cron_duplicate_groups(array $crons): array
    {
        $groups = [];

        foreach ($crons as $timestamp => $hooks) {
            if (!is_array($hooks)) {
                continue;
            }

            foreach ($hooks as $hook => $events) {
                if (!is_array($events)) {
                    continue;
                }

                foreach ($events as $event) {
                    if (!is_array($event)) {
                        continue;
                    }

                    $signature = Cron_Event_Filter::signature((string) $hook, $event, (int) $timestamp);
                    if (!isset($groups[$signature])) {
                        $groups[$signature] = [
                            'hook'     => (string) $hook,
                            'schedule' => (string) ($event['schedule'] ?? ''),
                            'args'     => $event['args'] ?? [],
                            'events'   => [],
                        ];
                    }

                    $groups[$signature]['events'][] = ['timestamp' => (int) $timestamp];
                }
            }
        }

        foreach ($groups as $signature => $group) {
            usort($group['events'], static function (array $left, array $right): int {
                return $left['timestamp'] <=> $right['timestamp'];
            });
            $groups[$signature] = $group;
        }

        return $groups;
    }

    /**
     * Report pending, overdue, and failed Action Scheduler actions by hook/group.
     *
     * ## OPTIONS
     *
     * [--status=<status>]
     * : Restrict the report to one Action Scheduler status. Supported: pending, failed.
     *
     * [--overdue]
     * : Show only overdue pending actions.
     *
     * [--limit=<limit>]
     * : Maximum grouped rows per section.
     * ---
     * default: 20
     * ---
     *
     * [--samples=<samples>]
     * : Number of sample action IDs per grouped row. Use 0 to hide samples.
     * ---
     * default: 5
     * ---
     *
     * ## EXAMPLES
     *
     *     wp queue as-report
     *     wp queue as-report --status=failed --limit=50
     *     wp queue as-report --overdue
     *
     * @subcommand as-report
     */
    public function as_report($args, $assoc_args): void
    {
        $status = isset($assoc_args['status']) ? strtolower((string) $assoc_args['status']) : '';
        $overdue_only = (bool) ($assoc_args['overdue'] ?? false);
        $limit = max(1, (int) ($assoc_args['limit'] ?? 20));
        $sample_limit = max(0, (int) ($assoc_args['samples'] ?? 5));

        if ($status !== '' && !in_array($status, ['pending', 'failed'], true)) {
            WP_CLI::error('Unsupported status. Use pending or failed.');
        }

        if ($overdue_only && $status === 'failed') {
            WP_CLI::error('--overdue can only be used with pending actions.');
        }

        $tables = $this->action_scheduler_table_sets();
        if (empty($tables)) {
            WP_CLI::warning('No Action Scheduler tables found for the current site/network.');
            return;
        }

        if (!class_exists('ActionScheduler') && !function_exists('as_get_scheduled_actions')) {
            WP_CLI::warning('Action Scheduler is not loaded; reporting directly from existing tables.');
        }

        $sections = [];
        if ($overdue_only) {
            $sections[] = ['label' => 'Overdue pending Action Scheduler actions', 'status' => 'pending', 'overdue' => true];
        } elseif ($status === 'pending') {
            $sections[] = ['label' => 'Pending Action Scheduler actions', 'status' => 'pending', 'overdue' => false];
        } elseif ($status === 'failed') {
            $sections[] = ['label' => 'Failed Action Scheduler actions', 'status' => 'failed', 'overdue' => false];
        } else {
            $sections[] = ['label' => 'Pending Action Scheduler actions', 'status' => 'pending', 'overdue' => false];
            $sections[] = ['label' => 'Overdue pending Action Scheduler actions', 'status' => 'pending', 'overdue' => true];
            $sections[] = ['label' => 'Failed Action Scheduler actions', 'status' => 'failed', 'overdue' => false];
        }

        foreach ($sections as $section) {
            $rows = $this->action_scheduler_report_rows(
                $tables,
                $section['status'],
                $section['overdue'],
                $limit,
                $sample_limit
            );

            WP_CLI::log('');
            WP_CLI::log((string) $section['label']);

            if (empty($rows)) {
                WP_CLI::log('  None found.');
                continue;
            }

            \WP_CLI\Utils\format_items('table', $rows, array_keys($rows[0]));
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
     * @return array<int, array{source: string, actions: string, groups: string}>
     */
    private function action_scheduler_table_sets(): array
    {
        global $wpdb;

        $prefixes = [];
        $prefixes[$wpdb->prefix] = 'current';

        if (is_multisite()) {
            $prefixes[$wpdb->base_prefix] = $prefixes[$wpdb->base_prefix] ?? 'network';

            foreach (get_sites(['number' => 0, 'fields' => 'ids']) as $site_id) {
                $prefix = $wpdb->get_blog_prefix((int) $site_id);
                $prefixes[$prefix] = $prefixes[$prefix] ?? 'site ' . (int) $site_id;
            }
        }

        $tables = [];
        foreach ($prefixes as $prefix => $source) {
            $actions_table = $prefix . 'actionscheduler_actions';
            $groups_table = $prefix . 'actionscheduler_groups';

            if ($this->database_table_exists($actions_table) && $this->database_table_exists($groups_table)) {
                $tables[$actions_table] = [
                    'source'  => $source,
                    'actions' => $actions_table,
                    'groups'  => $groups_table,
                ];
            }
        }

        return array_values($tables);
    }

    private function database_table_exists(string $table): bool
    {
        global $wpdb;

        $found = $wpdb->get_var($wpdb->prepare('SHOW TABLES LIKE %s', $wpdb->esc_like($table)));

        return $found === $table;
    }

    /**
     * @param array<int, array{source: string, actions: string, groups: string}> $tables
     * @return array<int, array<string, string|int>>
     */
    private function action_scheduler_report_rows(array $tables, string $status, bool $overdue, int $limit, int $sample_limit): array
    {
        global $wpdb;

        $rows = [];
        $now_gmt = current_time('mysql', true);

        foreach ($tables as $table_set) {
            $actions_table = $this->quote_identifier($table_set['actions']);
            $groups_table = $this->quote_identifier($table_set['groups']);
            $where = 'a.status = %s';
            $params = [$status];

            if ($overdue) {
                $where .= ' AND a.scheduled_date_gmt < %s';
                $params[] = $now_gmt;
            }

            $params[] = $limit;

            $sql = $wpdb->prepare(
                "SELECT a.hook, COALESCE(g.slug, '') AS group_slug, COUNT(*) AS action_count, MIN(a.scheduled_date_gmt) AS oldest_date, MAX(a.scheduled_date_gmt) AS latest_date
                FROM {$actions_table} a
                LEFT JOIN {$groups_table} g ON g.group_id = a.group_id
                WHERE {$where}
                GROUP BY a.hook, g.slug
                ORDER BY action_count DESC, oldest_date ASC
                LIMIT %d",
                $params
            );

            foreach ($wpdb->get_results($sql, ARRAY_A) as $result) {
                $row = [
                    'source'      => $table_set['source'],
                    'hook'        => (string) $result['hook'],
                    'group'       => $result['group_slug'] !== '' ? (string) $result['group_slug'] : '(none)',
                    'count'       => (int) $result['action_count'],
                    'oldest_date' => (string) $result['oldest_date'],
                    'latest_date' => (string) $result['latest_date'],
                ];

                if ($sample_limit > 0) {
                    $row['sample_ids'] = $this->sample_action_ids(
                        $table_set['actions'],
                        $table_set['groups'],
                        (string) $result['hook'],
                        (string) $result['group_slug'],
                        $status,
                        $overdue,
                        $now_gmt,
                        $sample_limit
                    );
                }

                $rows[] = $row;
            }
        }

        usort($rows, static function (array $left, array $right): int {
            return $right['count'] <=> $left['count'] ?: strcmp((string) $left['oldest_date'], (string) $right['oldest_date']);
        });

        return array_slice($rows, 0, $limit);
    }

    private function sample_action_ids(string $actions_table, string $groups_table, string $hook, string $group_slug, string $status, bool $overdue, string $now_gmt, int $limit): string
    {
        global $wpdb;

        $actions_table = $this->quote_identifier($actions_table);
        $groups_table = $this->quote_identifier($groups_table);
        $where = 'a.hook = %s AND a.status = %s AND COALESCE(g.slug, \'\') = %s';
        $params = [$hook, $status, $group_slug];

        if ($overdue) {
            $where .= ' AND a.scheduled_date_gmt < %s';
            $params[] = $now_gmt;
        }

        $params[] = $limit;
        $sql = $wpdb->prepare(
            "SELECT a.action_id
            FROM {$actions_table} a
            LEFT JOIN {$groups_table} g ON g.group_id = a.group_id
            WHERE {$where}
            ORDER BY a.scheduled_date_gmt ASC, a.action_id ASC
            LIMIT %d",
            $params
        );

        return implode(',', array_map('strval', $wpdb->get_col($sql)));
    }

    private function quote_identifier(string $identifier): string
    {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $identifier)) {
            WP_CLI::error('Unsafe database identifier encountered.');
        }

        return '`' . $identifier . '`';
    }

    /**
     * Restart the queue worker (sends SIGTERM, systemd auto-restarts).
     *
     * ## EXAMPLES
     *
     *     wp queue restart
     *
     * @subcommand restart
     */
    public function restart($args, $assoc_args): void
    {
        if (!Socket_Client::is_worker_running()) {
            WP_CLI::error('Queue worker is not running.');
        }

        // send_command won't get a response since worker stops immediately
        $path = Socket_Client::get_socket_path();
        $socket = @stream_socket_client('unix://' . $path, $errno, $errstr, 2);
        if (!$socket) {
            WP_CLI::error("Cannot connect to worker: $errstr");
        }
        fwrite($socket, json_encode(['command' => 'restart']) . "\n");
        fclose($socket);

        WP_CLI::success('Sent restart signal to queue worker. systemd will restart it.');
    }

}
