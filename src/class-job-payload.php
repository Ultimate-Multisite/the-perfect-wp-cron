<?php

namespace QueueWorker;

class Job_Payload
{
    private const ISOLATED_SITE_FACTOR = 4294967296;

    public int $site_id;
    public int $isolated_network_id;
    public int $local_site_id;
    public string $site_url;
    public string $hook;
    public array $args;
    public int $timestamp;
    public string $schedule;
    public int $interval;
    public string $source; // 'wp_cron' | 'action_scheduler'
    public int $action_id;
    public string $group;
    public string $lane;

    public function __construct(array $data = [])
    {
        $this->isolated_network_id = (int) ($data['isolated_network_id'] ?? self::current_isolated_network_id());
        $this->local_site_id = (int) ($data['local_site_id'] ?? ($this->isolated_network_id > 0 ? get_current_blog_id() : 0));
        $this->site_id   = (int) ($data['site_id'] ?? self::current_site_id());
        $this->site_url  = $data['site_url'] ?? self::current_site_url();
        $this->hook      = $data['hook'] ?? '';
        $this->args      = $data['args'] ?? [];
        $this->timestamp = $data['timestamp'] ?? 0;
        $this->schedule  = $data['schedule'] ?? '';
        $this->interval  = $data['interval'] ?? 0;
        $this->source    = $data['source'] ?? 'wp_cron';
        $this->action_id = $data['action_id'] ?? 0;
        $this->group     = $data['group'] ?? '';
        $this->lane      = $data['lane'] ?? ($this->source === 'action_scheduler' ? 'action_scheduler' : 'wp_cron');

        if (($this->isolated_network_id > 0) !== ($this->local_site_id > 0)) {
            throw new \InvalidArgumentException('Incomplete isolated network routing metadata');
        }

        if ($this->isolated_network_id > 0
            && $this->site_id !== self::isolated_site_id($this->isolated_network_id, $this->local_site_id)
        ) {
            throw new \InvalidArgumentException('Isolated queue site identity does not match its routing metadata');
        }
    }

    public function to_json(): string
    {
        return json_encode([
            'site_id'             => $this->site_id,
            'isolated_network_id' => $this->isolated_network_id,
            'local_site_id'       => $this->local_site_id,
            'site_url'            => $this->site_url,
            'hook'                => $this->hook,
            'args'                => $this->args,
            'timestamp'           => $this->timestamp,
            'schedule'            => $this->schedule,
            'interval'            => $this->interval,
            'source'              => $this->source,
            'action_id'           => $this->action_id,
            'group'               => $this->group,
            'lane'                => $this->lane,
        ]);
    }

    public static function from_json(string $json): self
    {
        $data = json_decode($json, true);
        if (!is_array($data)) {
            throw new \InvalidArgumentException('Invalid JSON payload');
        }
        return new self($data);
    }

    public static function from_cron_event(object $event): self
    {
        $schedules = wp_get_schedules();
        $interval  = 0;
        if (!empty($event->schedule) && isset($schedules[$event->schedule])) {
            $interval = (int) $schedules[$event->schedule]['interval'];
        }

        return new self([
            'site_id'   => self::current_site_id(),
            'site_url'  => self::current_site_url(),
            'hook'      => $event->hook,
            'args'      => $event->args ?? [],
            'timestamp' => (int) $event->timestamp,
            'schedule'  => $event->schedule ?? '',
            'interval'  => $interval,
            'source'    => 'wp_cron',
            'group'     => '',
        ]);
    }

    public static function from_as_action(int $action_id): ?self
    {
        if (!function_exists('as_get_scheduled_actions')) {
            return null;
        }

        $store  = \ActionScheduler::store();
        $action = $store->fetch_action($action_id);
        if ($action->is_finished()) {
            return null;
        }

        $schedule  = $action->get_schedule();
        $next_date = $schedule->get_date();
        $timestamp = $next_date ? $next_date->getTimestamp() : time();

        return new self([
            'site_id'   => self::current_site_id(),
            'site_url'  => self::current_site_url(),
            'hook'      => $action->get_hook(),
            'args'      => $action->get_args(),
            'timestamp' => $timestamp,
            'source'    => 'action_scheduler',
            'action_id' => $action_id,
            'group'     => method_exists($action, 'get_group') ? $action->get_group() : '',
        ]);
    }

    public function tracking_key(): string
    {
        if ($this->is_action_scheduler() && $this->action_id > 0) {
            return sprintf(
                'action_scheduler:%d:%d',
                $this->site_id,
                $this->action_id
            );
        }

        return sprintf(
            '%s:%d:%s:%s:%s:%d',
            $this->source,
            $this->site_id,
            $this->hook,
            $this->group,
            md5(serialize($this->args)),
            $this->timestamp
        );
    }

    public function is_action_scheduler(): bool
    {
        return $this->source === 'action_scheduler';
    }

    public function is_recurring(): bool
    {
        return $this->schedule !== '' || $this->interval > 0;
    }

    public function is_one_shot(): bool
    {
        return !$this->is_recurring();
    }

    public function source_metadata(): string
    {
        if ($this->is_action_scheduler()) {
            return $this->group !== '' ? 'as:' . $this->group : 'as';
        }

        return $this->is_recurring() ? 'wp_cron:recurring' : 'wp_cron:one-shot';
    }

    public static function isolated_site_id(int $network_id, int $local_site_id): int
    {
        if (PHP_INT_SIZE < 8 || $network_id < 1 || $network_id > 2147483647
            || $local_site_id < 1 || $local_site_id > 4294967295
        ) {
            throw new \InvalidArgumentException('Isolated network and local site IDs exceed the queue identity range');
        }

        return ($network_id * self::ISOLATED_SITE_FACTOR) + $local_site_id;
    }

    public function routes_to_isolated_network(int $network_id): bool
    {
        if ($network_id < 1 || $this->isolated_network_id !== $network_id || $this->local_site_id < 1) {
            return false;
        }

        return $this->site_id === self::isolated_site_id($network_id, $this->local_site_id);
    }

    private static function current_site_id(): int
    {
        if (defined('WU_MT_SOVEREIGN_TENANT')) {
            return (int) WU_MT_SOVEREIGN_TENANT;
        }

        $isolated_network_id = self::current_isolated_network_id();
        if ($isolated_network_id > 0) {
            return self::isolated_site_id($isolated_network_id, get_current_blog_id());
        }

        if (function_exists('ms_is_switched') && ms_is_switched()) {
            return get_current_blog_id();
        }

        $host = strtolower(trim((string) ($_SERVER['HTTP_HOST'] ?? '')));
        if ($host !== '' && defined('WP_CONTENT_DIR')) {
            $path = WP_CONTENT_DIR . '/site-registry.data.json';
            if (is_readable($path)) {
                $data = json_decode((string) file_get_contents($path), true);
                if (is_array($data) && isset($data['domain_index'][$host])) {
                    return (int) $data['domain_index'][$host];
                }
            }
        }

        return get_current_blog_id();
    }

    private static function current_isolated_network_id(): int
    {
        if (defined('WU_MT_SOVEREIGN_TENANT') || !defined('WU_MT_LEGACY_ISOLATED_NETWORK')) {
            return 0;
        }

        return (int) WU_MT_LEGACY_ISOLATED_NETWORK;
    }

    /**
     * Return a URL that WordPress can use to bootstrap the current site.
     *
     * During a full-network scan, get_site_url() can be filtered to a mapped,
     * stale, or secondary-network domain. Bootstrapping a fresh executor with
     * that URL can select a different routed database before switch_to_blog()
     * runs. Prefer a uniquely owned active domain mapping. Fall back to the
     * wp_blogs domain only when another blog has not claimed it.
     */
    private static function current_site_url(): string
    {
        $site_url = get_site_url();

        if (!function_exists('ms_is_switched') || !ms_is_switched() || !function_exists('get_site')) {
            return $site_url;
        }

        $site = get_site(get_current_blog_id());
        if (!$site || empty($site->domain)) {
            return $site_url;
        }

        $scheme = (string) parse_url($site_url, PHP_URL_SCHEME);
        if (!in_array($scheme, ['http', 'https'], true)) {
            $scheme = 'https';
        }

        $path = (string) ($site->path ?? '/');
        if ($path === '') {
            $path = '/';
        } elseif ($path[0] !== '/') {
            $path = '/' . $path;
        }

        $site_id = get_current_blog_id();
        $mapped_url = self::current_mapped_site_url($site_id, $path, $scheme === 'https');
        if ($mapped_url !== '') {
            return $mapped_url;
        }

        if (self::canonical_domain_is_mapped_elsewhere((string) $site->domain, $site_id)) {
            return '';
        }

        return $scheme . '://' . (string) $site->domain . $path;
    }

    /**
     * Return a uniquely owned active mapping for a switched blog.
     */
    private static function current_mapped_site_url(int $site_id, string $path, bool $secure): string
    {
        global $wpdb;

        $table = $wpdb->base_prefix . 'wu_domain_mappings';
        if (!preg_match('/^[A-Za-z0-9_]+$/', $table)) {
            return '';
        }

        $found = $wpdb->get_var($wpdb->prepare('SHOW TABLES LIKE %s', $wpdb->esc_like($table)));
        if ($found !== $table) {
            return '';
        }

        $sql = 'SELECT candidate.domain, candidate.secure FROM `' . $table . '` candidate '
            . 'WHERE candidate.blog_id = %d AND candidate.active = 1 '
            . 'AND NOT EXISTS (SELECT 1 FROM `' . $table . '` conflict '
            . 'WHERE conflict.domain = candidate.domain AND conflict.active = 1 '
            . 'AND conflict.blog_id <> candidate.blog_id) '
            . 'ORDER BY candidate.primary_domain DESC, candidate.id DESC LIMIT 1';
        $mapping = $wpdb->get_row($wpdb->prepare($sql, $site_id));
        if (!is_object($mapping) || empty($mapping->domain)) {
            return '';
        }

        $domain = strtolower(rtrim(trim((string) $mapping->domain), '.'));
        if (preg_match('/^[a-z0-9.-]+(?::[0-9]+)?$/', $domain) !== 1) {
            return '';
        }

        return (!empty($mapping->secure) || $secure ? 'https://' : 'http://') . $domain . $path;
    }

    /**
     * Check whether a canonical domain is actively mapped to another blog.
     */
    private static function canonical_domain_is_mapped_elsewhere(string $domain, int $site_id): bool
    {
        global $wpdb;

        $table = $wpdb->base_prefix . 'wu_domain_mappings';
        if (!preg_match('/^[A-Za-z0-9_]+$/', $table)) {
            return false;
        }

        $found = $wpdb->get_var($wpdb->prepare('SHOW TABLES LIKE %s', $wpdb->esc_like($table)));
        if ($found !== $table) {
            return false;
        }

        $sql = 'SELECT COUNT(*) FROM `' . $table . '` '
            . 'WHERE domain = %s AND active = 1 AND blog_id <> %d';
        $conflicts = $wpdb->get_var($wpdb->prepare(
            $sql,
            strtolower(rtrim(trim($domain), '.')),
            $site_id
        ));

        return (int) $conflicts > 0;
    }
}
