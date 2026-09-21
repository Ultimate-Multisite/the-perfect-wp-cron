<?php

namespace QueueWorker;

class Action_Scheduler_Bridge
{
    public const CLEANUP_HOOK = 'qw_cleanup_action_scheduler';
    private const CLEANUP_SCHEDULE = 'qw_every_five_minutes';
    private const CLEANUP_BATCH_SIZE = 100;
    private static bool $stored_action_hook_registered = false;

    public static function register(): void
    {
        // Remove the default AS queue runner — the worker handles execution
        if (class_exists('ActionScheduler_QueueRunner')) {
            remove_action(
                'action_scheduler_run_queue',
                [\ActionScheduler_QueueRunner::instance(), 'run']
            );
        }

        self::register_stored_action_hook();
        add_filter('cron_schedules', [__CLASS__, 'cleanup_schedule']);
        add_action(self::CLEANUP_HOOK, [__CLASS__, 'cleanup']);

        // process_action() bypasses QueueRunner::run(), including its cleaner.
        // Use a worker-managed cron event, not a second AS queue runner.
        if (!self::has_native_cleanup() && !wp_next_scheduled(self::CLEANUP_HOOK)) {
            wp_schedule_event(time() + 300, self::CLEANUP_SCHEDULE, self::CLEANUP_HOOK);
        }
    }

    public static function cleanup_schedule(array $schedules): array
    {
        $schedules[self::CLEANUP_SCHEDULE] = [
            'interval' => 300,
            'display'  => 'Queue worker maintenance every five minutes',
        ];
        return $schedules;
    }

    private static function has_native_cleanup(): bool
    {
        // Newer AS integrations can install their own independent WP-Cron
        // cleaner. Do not duplicate it, but do not trust an orphaned event.
        return has_action('action_scheduler_run_actions_cleanup_hook') !== false
            && wp_next_scheduled('action_scheduler_run_actions_cleanup_hook') !== false;
    }

    public static function cleanup(): void
    {
        if (self::has_native_cleanup()) {
            return;
        }
        if (!class_exists('ActionScheduler_QueueCleaner')) {
            throw new \RuntimeException('Action Scheduler cleaner is unavailable');
        }

        $retention = (int) apply_filters('action_scheduler_retention_period', 2678400);
        if ($retention <= 0) {
            throw new \RuntimeException('Action Scheduler cleanup requires a positive retention period');
        }
        $allowed = ['complete', 'canceled'];
        $statuses = array_values(array_intersect(
            $allowed,
            (array) apply_filters('action_scheduler_default_cleaner_statuses', $allowed)
        ));
        if ($statuses === []) {
            return;
        }

        // Exactly one bounded pass: at most 100 per terminal status. Never
        // reset claims, mark timeouts failed, or delete pending/failed actions.
        $cleaner = new \ActionScheduler_QueueCleaner();
        $cleaner->clean_actions(
            $statuses,
            as_get_datetime_object(time() - $retention),
            self::CLEANUP_BATCH_SIZE
        );
    }

    public static function register_stored_action_hook(): void
    {
        if (self::$stored_action_hook_registered) {
            return;
        }

        add_action('action_scheduler_stored_action', [__CLASS__, 'on_stored_action']);
        self::$stored_action_hook_registered = true;
    }

    public static function on_stored_action(int $action_id): void
    {
        $payload = Job_Payload::from_as_action($action_id);
        if ($payload === null) {
            return;
        }

        Socket_Client::notify($payload);
    }
}
