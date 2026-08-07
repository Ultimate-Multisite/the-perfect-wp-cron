<?php
/**
 * Scan a site's WP-Cron array in a fresh WordPress environment.
 *
 * Used by the long-running worker for sovereign tenants. Those tenants must be
 * bootstrapped by domain so db-config.php selects the tenant DB and wp_ table
 * prefix before WordPress loads.
 */

// --- Parse payload from stdin or CLI arg ---
if (($argv[1] ?? '') === '--stdin') {
    $raw = stream_get_contents(STDIN);
} else {
    $raw = base64_decode($argv[1] ?? '', true);
}

if (!$raw) {
    fwrite(STDERR, "Missing or invalid payload.\n");
    exit(1);
}

$payload = json_decode($raw, true);
if (!is_array($payload)) {
    fwrite(STDERR, "Invalid JSON payload.\n");
    exit(1);
}

// WordPress and plugins can emit notices while bootstrapping. The parent
// process consumes stdout as a JSON-only protocol, so keep every incidental
// byte out of that stream until the payload is ready to write.
$stdout_buffer_level = ob_get_level();
ob_start();

// --- Load QueueWorker classes ---
$plugin_autoload = dirname(__DIR__) . '/vendor/autoload.php';
if (file_exists($plugin_autoload)) {
    require_once $plugin_autoload;
}
if (!class_exists('QueueWorker\\Bootstrap')) {
    require_once dirname(__DIR__) . '/src/class-bootstrap.php';
}

// Discover WordPress before loading the site Composer autoloader. Bedrock's
// autoloaded plugin files may exit when ABSPATH has not been defined yet.
$wp_load = QueueWorker\Bootstrap::discover_wp_load(__DIR__);
if (!defined('ABSPATH')) {
    define('ABSPATH', rtrim(dirname($wp_load), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR);
}

$site_autoload = null;
$search = dirname(__DIR__);
for ($i = 0; $i < 10; $i++) {
    $search = dirname($search);
    if (file_exists($search . '/vendor/autoload.php')) {
        $site_autoload = $search . '/vendor/autoload.php';
        break;
    }
}

if ($site_autoload) {
    require_once $site_autoload;
}

if (!class_exists('QueueWorker\\Config')) {
    fwrite(STDERR, "Could not find QueueWorker classes.\n");
    exit(1);
}

use QueueWorker\Bootstrap;
use QueueWorker\Cron_Event_Filter;
use QueueWorker\Job_Payload;

$site_root = $site_autoload ? dirname($site_autoload, 2) : dirname(__DIR__);
Bootstrap::load_dotenv($site_root);

$domain = parse_url($payload['site_url'] ?? '', PHP_URL_HOST);
if (!$domain) {
    fwrite(STDERR, "Missing site_url host.\n");
    exit(1);
}

$_SERVER['HTTP_HOST']      = $domain;
$_SERVER['SERVER_NAME']    = $domain;
$_SERVER['REQUEST_URI']    = '/';
$_SERVER['SERVER_PORT']    = '443';
$_SERVER['HTTPS']          = 'on';
$_SERVER['REQUEST_METHOD'] = 'GET';

define('QUEUE_WORKER_RUNNING', true);

require_once $wp_load;

$payloads = [];
$isolated_network_id = (int) ($payload['isolated_network_id'] ?? 0);
if ($isolated_network_id > 0) {
    if (!defined('WU_MT_LEGACY_ISOLATED_NETWORK')
        || (int) WU_MT_LEGACY_ISOLATED_NETWORK !== $isolated_network_id
    ) {
        fwrite(STDERR, sprintf(
            "Isolated network mismatch: domain routed to network %d, expected network %d.\n",
            defined('WU_MT_LEGACY_ISOLATED_NETWORK') ? (int) WU_MT_LEGACY_ISOLATED_NETWORK : 0,
            $isolated_network_id
        ));
        exit(1);
    }

    $initial_blog_id = get_current_blog_id();
    $site_ids = is_multisite() ? get_sites(['number' => 0, 'fields' => 'ids']) : [$initial_blog_id];
    foreach ($site_ids as $local_site_id) {
        $local_site_id = (int) $local_site_id;
        $switched = $local_site_id !== get_current_blog_id();
        if ($switched) {
            switch_to_blog($local_site_id);
        }

        $payloads = array_merge($payloads, qw_scan_current_site_jobs());

        if ($switched) {
            restore_current_blog();
        }
    }

    if ($initial_blog_id !== get_current_blog_id()) {
        switch_to_blog($initial_blog_id);
    }
} else {
    $site_id = (int) ($payload['site_id'] ?? get_current_blog_id());
    $is_sovereign_tenant = (defined('WU_MT_SOVEREIGN_TENANT') && (int) WU_MT_SOVEREIGN_TENANT === $site_id)
        || qw_scan_payload_matches_sovereign_registry($site_id, $domain);
    if (!$is_sovereign_tenant && $site_id !== get_current_blog_id()) {
        switch_to_blog($site_id);
    }

    $payloads = qw_scan_current_site_jobs();
}

// A plugin can leave nested output buffers open. Discard every buffer opened
// during this scan before writing the protocol response to real stdout.
$incidental_output = '';
while (ob_get_level() > $stdout_buffer_level) {
    $incidental_output .= (string) ob_get_clean();
}

if ($incidental_output !== '') {
    fwrite(STDERR, sprintf(
        "Suppressed %d bytes of bootstrap output (sha256:%s).\n",
        strlen($incidental_output),
        substr(hash('sha256', $incidental_output), 0, 12)
    ));
}

try {
    $encoded_payloads = json_encode($payloads, JSON_THROW_ON_ERROR);
} catch (\JsonException $e) {
    fwrite(STDERR, sprintf("Could not encode scan payload: %s.\n", $e->getMessage()));
    exit(1);
}

fwrite(STDOUT, $encoded_payloads);

function qw_scan_current_site_jobs(): array
{
    wp_cache_delete('cron', 'options');
    wp_cache_delete('alloptions', 'options');

    $payloads = [];
    $crons = _get_cron_array();
    if (is_array($crons)) {
        $seen_cron_signatures = [];
        foreach ($crons as $timestamp => $hooks) {
            if (!is_array($hooks)) {
                continue;
            }
            foreach ($hooks as $hook => $events) {
                if (Cron_Event_Filter::should_bypass($hook)) {
                    continue;
                }
                foreach ($events as $event) {
                    $signature = qw_scan_cron_event_signature($hook, $event, (int) $timestamp);
                    if (isset($seen_cron_signatures[$signature])) {
                        continue;
                    }
                    $seen_cron_signatures[$signature] = true;

                    $event_obj = (object) array_merge($event, [
                        'hook'      => $hook,
                        'timestamp' => $timestamp,
                    ]);
                    $payloads[] = json_decode(Job_Payload::from_cron_event($event_obj)->to_json(), true);
                }
            }
        }
    }

    if (function_exists('as_get_scheduled_actions') && qw_scan_action_scheduler_tables_exist()) {
        try {
            $actions = as_get_scheduled_actions([
                'status'   => \ActionScheduler_Store::STATUS_PENDING,
                'per_page' => 500,
            ]);
            foreach ($actions as $action_id => $action) {
                $action_payload = Job_Payload::from_as_action($action_id);
                if ($action_payload) {
                    $payloads[] = json_decode($action_payload->to_json(), true);
                }
            }
        } catch (\Throwable $e) {
            fwrite(STDERR, "Action Scheduler scan failed: " . $e->getMessage() . "\n");
        }
    }

    return $payloads;
}

function qw_scan_action_scheduler_tables_exist(): bool
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

function qw_scan_cron_event_signature(string $hook, array $event, int $timestamp): string
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

function qw_scan_payload_matches_sovereign_registry(int $site_id, string $domain): bool
{
    if (!defined('WP_CONTENT_DIR') || $domain === '') {
        return false;
    }

    $path = WP_CONTENT_DIR . '/site-registry.data.json';
    if (!is_readable($path)) {
        return false;
    }

    $data = json_decode((string) file_get_contents($path), true);
    if (!is_array($data) || empty($data['sites'][$site_id]) || !is_array($data['sites'][$site_id])) {
        return false;
    }

    $entry = $data['sites'][$site_id];
    if (($entry['isolation_model'] ?? '') !== 'sovereign') {
        return false;
    }

    $domains = array_map('strtolower', array_map('strval', $entry['domains'] ?? []));
    return in_array(strtolower($domain), $domains, true);
}
