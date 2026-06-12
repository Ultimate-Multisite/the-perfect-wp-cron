<?php

namespace QueueWorker;

class Action_Scheduler_Bridge
{
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
