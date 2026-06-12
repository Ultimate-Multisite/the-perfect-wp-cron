<?php

namespace QueueWorker;

class Cron_Event_Filter
{
    public static function should_bypass(string $hook): bool
    {
        return in_array($hook, Config::bypass_cron_hooks(), true);
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
