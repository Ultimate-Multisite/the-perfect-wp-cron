<?php

namespace QueueWorker;

class Config
{
    public static function socket_path(): string
    {
        return self::get('QUEUE_WORKER_SOCKET_PATH', '/tmp/the-perfect-wp-cron.sock');
    }

    public static function worker_count(): int
    {
        return (int) self::get('QUEUE_WORKER_COUNT', 2);
    }

    public static function max_concurrent(): int
    {
        return (int) self::get('QUEUE_WORKER_MAX_CONCURRENT', 1);
    }

    public static function action_scheduler_max_concurrent(): int
    {
        return (int) self::get('QUEUE_WORKER_AS_MAX_CONCURRENT', 1);
    }

    public static function action_scheduler_max_batch_size(): int
    {
        return (int) self::get('QUEUE_WORKER_AS_MAX_BATCH_SIZE', 10);
    }

    public static function action_scheduler_lanes(): array
    {
        $value = self::get('QUEUE_WORKER_AS_LANES', []);
        if (is_string($value)) {
            if (trim($value) === '') {
                return [];
            }
            $value = json_decode($value, true);
        }

        if (!is_array($value)) {
            return [];
        }

        $normalized = [];
        foreach ($value as $lane) {
            if (!is_array($lane) || empty($lane['name'])) {
                continue;
            }

            $normalized[] = [
                'name'           => self::sanitize_lane_name((string) $lane['name']),
                'sites'          => self::normalize_int_list($lane['sites'] ?? $lane['site_ids'] ?? []),
                'groups'         => self::normalize_string_list($lane['groups'] ?? []),
                'hooks'          => self::normalize_string_list($lane['hooks'] ?? []),
                'max_concurrent' => max(1, (int) ($lane['max_concurrent'] ?? self::action_scheduler_max_concurrent())),
                'max_batch_size' => max(1, (int) ($lane['max_batch_size'] ?? self::action_scheduler_max_batch_size())),
            ];
        }

        return $normalized;
    }

    public static function urgent_hooks(): array
    {
        return self::normalize_string_list(self::get('QUEUE_WORKER_URGENT_HOOKS', []));
    }

    public static function low_priority_hooks(): array
    {
        return self::normalize_string_list(self::get('QUEUE_WORKER_LOW_PRIORITY_HOOKS', []));
    }

    public static function max_batch_size(): int
    {
        return (int) self::get('QUEUE_WORKER_MAX_BATCH_SIZE', 50);
    }

    public static function job_timeout(): int
    {
        return (int) self::get('QUEUE_WORKER_JOB_TIMEOUT', 300);
    }

    public static function batch_timeout(): int
    {
        return (int) self::get('QUEUE_WORKER_BATCH_TIMEOUT', 3600);
    }

    public static function rescan_interval(): int
    {
        return (int) self::get('QUEUE_WORKER_RESCAN_INTERVAL', 60);
    }

    public static function action_scheduler_rescan_interval(): int
    {
        return max(1, (int) self::get('QUEUE_WORKER_AS_RESCAN_INTERVAL', 5));
    }

    public static function memory_limit(): int
    {
        return (int) self::get('QUEUE_WORKER_MEMORY_LIMIT', 200);
    }

    public static function uptime_limit(): int
    {
        return (int) self::get('QUEUE_WORKER_UPTIME_LIMIT', 3600);
    }

    public static function log_file(): string
    {
        $value = self::get('QUEUE_WORKER_LOG_FILE', '');
        if ($value !== '') {
            return $value;
        }
        // Auto-detect: Workerman default log location
        if (defined('ABSPATH')) {
            $dir = WP_CONTENT_DIR . '/logs';
            if (is_dir($dir) && is_writable($dir)) {
                return $dir . '/the-perfect-wp-cron.log';
            }
        }
        return '/var/log/the-perfect-wp-cron.log';
    }

    public static function log_retention(): int
    {
        return (int) self::get('QUEUE_WORKER_LOG_RETENTION', 7);
    }

    private static function get(string $name, mixed $default): mixed
    {
        // PHP constant first
        if (defined($name)) {
            return constant($name);
        }
        // Environment variable
        $env = getenv($name);
        if ($env !== false && $env !== '') {
            return $env;
        }
        return $default;
    }

    private static function sanitize_lane_name(string $name): string
    {
        $name = preg_replace('/[^a-zA-Z0-9_.:-]+/', '-', trim($name));
        if ($name === '' || $name === 'wp_cron') {
            return 'action_scheduler';
        }

        return $name;
    }

    private static function normalize_int_list(mixed $value): array
    {
        $items = is_array($value) ? $value : [$value];
        $result = [];
        foreach ($items as $item) {
            if ($item === '' || $item === null) {
                continue;
            }
            $result[] = (int) $item;
        }

        return array_values(array_unique(array_filter($result)));
    }

    private static function normalize_string_list(mixed $value): array
    {
        if (is_string($value)) {
            $value = explode(',', $value);
        }

        $items = is_array($value) ? $value : [$value];
        $result = [];
        foreach ($items as $item) {
            $item = trim((string) $item);
            if ($item !== '') {
                $result[] = $item;
            }
        }

        return array_values(array_unique($result));
    }
}
