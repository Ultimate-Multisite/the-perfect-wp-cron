<?php

namespace QueueWorker;

class Cron_Interceptor
{
    public static function register(): void
    {
        add_filter('schedule_event', [__CLASS__, 'on_schedule_event']);
    }

    public static function on_schedule_event($event)
    {
        if (!is_object($event) || empty($event->hook)) {
            return $event;
        }

        if (Cron_Event_Filter::should_bypass($event->hook)) {
            return $event;
        }

        $payload = Job_Payload::from_cron_event($event);
        Socket_Client::notify($payload);

        return $event;
    }
}
