<?php

namespace QueueWorker;

class Cron_Event_Filter
{
    private const BYPASS_HOOKS = [
        'wp_version_check',
        'wp_update_plugins',
        'wp_update_themes',
        'action_scheduler_run_queue',
        'action_scheduler_run_cleanup',
    ];

    public static function should_bypass(string $hook): bool
    {
        return in_array($hook, self::BYPASS_HOOKS, true);
    }

    public static function signature(string $hook, array $event, int $timestamp): string
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
}
