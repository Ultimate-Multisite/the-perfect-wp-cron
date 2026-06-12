<?php

namespace QueueWorker;

class Job_Payload
{
    public int $site_id;
    public string $site_url;
    public string $hook;
    public array $args;
    public int $timestamp;
    public string $schedule;
    public int $interval;
    public string $source; // 'wp_cron' | 'action_scheduler'
    public int $action_id;
    public string $group;

    public function __construct(array $data = [])
    {
        $this->site_id   = $data['site_id'] ?? self::current_site_id();
        $this->site_url  = $data['site_url'] ?? get_site_url();
        $this->hook      = $data['hook'] ?? '';
        $this->args      = $data['args'] ?? [];
        $this->timestamp = $data['timestamp'] ?? 0;
        $this->schedule  = $data['schedule'] ?? '';
        $this->interval  = $data['interval'] ?? 0;
        $this->source    = $data['source'] ?? 'wp_cron';
        $this->action_id = $data['action_id'] ?? 0;
        $this->group     = $data['group'] ?? '';
    }

    public function to_json(): string
    {
        return json_encode([
            'site_id'   => $this->site_id,
            'site_url'  => $this->site_url,
            'hook'      => $this->hook,
            'args'      => $this->args,
            'timestamp' => $this->timestamp,
            'schedule'  => $this->schedule,
            'interval'  => $this->interval,
            'source'    => $this->source,
            'action_id' => $this->action_id,
            'group'     => $this->group,
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
            'site_url'  => get_site_url(),
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
            'site_url'  => get_site_url(),
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
        return sprintf(
            '%s:%d:%s:%s:%s:%s:%d',
            $this->source,
            $this->site_id,
            md5($this->site_url),
            $this->hook,
            $this->group,
            md5(serialize($this->args)),
            $this->timestamp
        );
    }

    private static function current_site_id(): int
    {
        if (defined('WU_MT_SOVEREIGN_TENANT')) {
            return (int) WU_MT_SOVEREIGN_TENANT;
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
}
