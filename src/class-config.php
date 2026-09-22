<?php

namespace QueueWorker;

class Config
{
    private const DEFAULT_BYPASS_CRON_HOOKS = [
        'wp_version_check',
        'wp_update_plugins',
        'wp_update_themes',
        'action_scheduler_run_queue',
        'action_scheduler_run_cleanup',
    ];

    public static function socket_path(): string
    {
        return self::get('QUEUE_WORKER_SOCKET_PATH', '/tmp/the-perfect-wp-cron.sock');
    }

    public static function runtime_dir(): string
    {
        return trim((string) self::get('QUEUE_WORKER_RUNTIME_DIR', ''));
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
                'name'           => self::sanitize_lane_name((string) $lane['name'], 'action_scheduler'),
                'sites'          => self::normalize_int_list($lane['sites'] ?? $lane['site_ids'] ?? []),
                'groups'         => self::normalize_string_list($lane['groups'] ?? []),
                'hooks'          => self::normalize_string_list($lane['hooks'] ?? []),
                'max_concurrent' => max(1, (int) ($lane['max_concurrent'] ?? self::action_scheduler_max_concurrent())),
                'max_batch_size' => max(1, (int) ($lane['max_batch_size'] ?? self::action_scheduler_max_batch_size())),
            ];
        }

        return $normalized;
    }

    public static function cron_max_concurrent(): int
    {
        return (int) self::get('QUEUE_WORKER_CRON_MAX_CONCURRENT', self::max_concurrent());
    }

    public static function cron_max_batch_size(): int
    {
        return (int) self::get('QUEUE_WORKER_CRON_MAX_BATCH_SIZE', self::max_batch_size());
    }

    public static function cron_lanes(): array
    {
        $value = self::get('QUEUE_WORKER_CRON_LANES', []);
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
                'name'           => self::sanitize_lane_name((string) $lane['name'], 'wp_cron'),
                'sites'          => self::normalize_int_list($lane['sites'] ?? $lane['site_ids'] ?? []),
                'hooks'          => self::normalize_string_list($lane['hooks'] ?? []),
                'max_concurrent' => max(1, (int) ($lane['max_concurrent'] ?? self::cron_max_concurrent())),
                'max_batch_size' => max(1, (int) ($lane['max_batch_size'] ?? self::cron_max_batch_size())),
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

    public static function bypass_cron_hooks(): array
    {
        $bypassed = array_values(array_unique(array_merge(
            self::DEFAULT_BYPASS_CRON_HOOKS,
            self::normalize_string_list(self::get('QUEUE_WORKER_BYPASS_CRON_HOOKS', []))
        )));

        $managed = self::normalize_string_list(self::get('QUEUE_WORKER_MANAGED_CRON_HOOKS', []));

        return array_values(array_diff($bypassed, $managed));
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

    public static function scheduling_horizon(): int
    {
        return max(1, self::rescan_interval(), (int) self::get('QUEUE_WORKER_SCHEDULING_HORIZON', 3600));
    }

    public static function scan_timeout(): int
    {
        return max(1, (int) self::get('QUEUE_WORKER_SCAN_TIMEOUT', 300));
    }

    public static function action_scheduler_rescan_interval(): int
    {
        return max(1, (int) self::get('QUEUE_WORKER_AS_RESCAN_INTERVAL', 5));
    }

    /**
     * Isolated networks owned by dedicated workers must not also be scanned by
     * the shared worker. The optional inventory file is re-read for every scan
     * so newly provisioned runtimes do not require a shared-worker restart.
     *
     * The file may be a JSON array of network IDs or an object containing a
     * "networks" object keyed by network ID.
     *
     * @return array<int>
     */
    public static function excluded_isolated_network_ids(): array
    {
        $configured_ids = self::get('QUEUE_WORKER_EXCLUDED_ISOLATED_NETWORK_IDS', []);
        if (is_string($configured_ids)) {
            $configured_ids = trim($configured_ids) === '' ? [] : explode(',', $configured_ids);
        }
        if (!is_array($configured_ids)) {
            throw new \RuntimeException('Dedicated-network exclusions must be a list of network IDs');
        }
        $network_ids = self::strict_network_ids($configured_ids);
        $path = trim((string) self::get('QUEUE_WORKER_EXCLUDED_ISOLATED_NETWORKS_FILE', ''));
        if ($path === '') {
            sort($network_ids, SORT_NUMERIC);
            return $network_ids;
        }

        if (!is_file($path) || !is_readable($path)) {
            throw new \RuntimeException('Dedicated-network inventory is unavailable');
        }

        $size = filesize($path);
        if ($size === false || $size > 16777216) {
            throw new \RuntimeException('Dedicated-network inventory has an invalid size');
        }

        $contents = file_get_contents($path);
        if ($contents === false) {
            throw new \RuntimeException('Dedicated-network inventory could not be read');
        }

        try {
            $data = json_decode($contents, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new \RuntimeException('Dedicated-network inventory is invalid JSON', 0, $e);
        }

        if (!is_array($data)) {
            throw new \RuntimeException('Dedicated-network inventory must be a JSON array or object');
        }

        if (str_starts_with(ltrim($contents), '[')) {
            if (!array_is_list($data)) {
                throw new \RuntimeException('Dedicated-network inventory ID list is invalid');
            }
            $file_ids = $data;
        } else {
            if (!isset($data['networks']) || !is_array($data['networks'])) {
                throw new \RuntimeException('Dedicated-network inventory is missing its networks object');
            }
            $file_ids = array_keys($data['networks']);
        }

        $network_ids = array_values(array_unique(array_merge(
            $network_ids,
            self::strict_network_ids($file_ids)
        )));
        sort($network_ids, SORT_NUMERIC);
        return $network_ids;
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

    private static function sanitize_lane_name(string $name, string $default): string
    {
        $name = preg_replace('/[^a-zA-Z0-9_.:-]+/', '-', trim($name));
        if ($name === '' || $name === 'wp_cron' || $name === 'action_scheduler') {
            return $default;
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

    private static function strict_network_ids(array $items): array
    {
        $network_ids = [];
        foreach ($items as $network_id) {
            if (!is_int($network_id) && !is_string($network_id)) {
                throw new \RuntimeException('Dedicated-network exclusions contain an invalid network ID');
            }
            $network_id = trim((string) $network_id);
            if (!preg_match('/^[1-9][0-9]*$/', $network_id)) {
                throw new \RuntimeException('Dedicated-network exclusions contain an invalid network ID');
            }
            $network_ids[] = (int) $network_id;
        }

        return array_values(array_unique($network_ids));
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
