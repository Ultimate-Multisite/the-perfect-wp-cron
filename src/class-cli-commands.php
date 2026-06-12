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
        WP_CLI::log(sprintf('  Pending AS:     %d', $data['pending_as_batches'] ?? 0));
        WP_CLI::log(sprintf('  Running jobs:   %d', $data['running_jobs'] ?? 0));
        WP_CLI::log(sprintf('  Running AS:     %d', $data['running_as_jobs'] ?? 0));
        WP_CLI::log(sprintf('  Memory:         %s', $data['memory'] ?? 'unknown'));

        if (!empty($data['rescan']) && is_array($data['rescan'])) {
            WP_CLI::log(sprintf(
                '  Rescan:         %s (last duration: %ds)',
                !empty($data['rescan']['in_progress']) ? 'in progress' : 'idle',
                (int) ($data['rescan']['last_duration'] ?? 0)
            ));
        }

        if (!empty($data['pending_as_lanes'])) {
            WP_CLI::log('  Pending AS lanes:');
            foreach ($data['pending_as_lanes'] as $lane => $count) {
                WP_CLI::log(sprintf('    - %s: %d', $lane, $count));
            }
        }

        if (!empty($data['running_as_lanes'])) {
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
