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

// --- Load QueueWorker classes ---
$plugin_autoload = dirname(__DIR__) . '/vendor/autoload.php';
if (file_exists($plugin_autoload)) {
    require_once $plugin_autoload;
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
use QueueWorker\Job_Payload;

$site_root = $site_autoload ? dirname($site_autoload, 2) : dirname(__DIR__);
Bootstrap::load_dotenv($site_root);
$wp_load = Bootstrap::discover_wp_load(__DIR__);

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

$site_id = (int) ($payload['site_id'] ?? get_current_blog_id());
$is_sovereign_tenant = (defined('WU_MT_SOVEREIGN_TENANT') && (int) WU_MT_SOVEREIGN_TENANT === $site_id)
    || qw_scan_payload_matches_sovereign_registry($site_id, $domain);
if (!$is_sovereign_tenant && $site_id !== get_current_blog_id()) {
    switch_to_blog($site_id);
}

wp_cache_delete('cron', 'options');
wp_cache_delete('alloptions', 'options');

$bypass_hooks = [
    'wp_version_check',
    'wp_update_plugins',
    'wp_update_themes',
    'action_scheduler_run_queue',
    'action_scheduler_run_cleanup',
];

$payloads = [];
$crons = _get_cron_array();
if (is_array($crons)) {
    foreach ($crons as $timestamp => $hooks) {
        if (!is_array($hooks)) {
            continue;
        }
        foreach ($hooks as $hook => $events) {
            if (in_array($hook, $bypass_hooks, true)) {
                continue;
            }
            foreach ($events as $event) {
                $event_obj = (object) array_merge($event, [
                    'hook'      => $hook,
                    'timestamp' => $timestamp,
                ]);
                $payloads[] = json_decode(Job_Payload::from_cron_event($event_obj)->to_json(), true);
            }
        }
    }
}

echo json_encode($payloads);

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
