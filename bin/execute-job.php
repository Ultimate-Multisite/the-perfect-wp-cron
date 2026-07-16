<?php
/**
 * Execute one or more jobs in a fresh WordPress environment.
 *
 * Spawned by worker.php as a subprocess. Bootstraps WordPress with the correct
 * site domain so all per-site plugins are loaded and DB tables are correct.
 * Accepts a single payload (legacy) or an array of payloads for batch execution.
 *
 * Usage:
 *   php execute-job.php --stdin              (reads JSON from stdin)
 *   php execute-job.php <base64-json>        (legacy)
 *
 * Exit codes:
 *   0 = all jobs succeeded
 *   1 = one or more jobs failed
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
$payload_data = json_decode($raw, true);
if (!is_array($payload_data)) {
    fwrite(STDERR, "Invalid JSON payload.\n");
    exit(1);
}

// Normalize: single payload (has 'hook' key) -> wrap in array
$payloads = isset($payload_data['hook']) ? [$payload_data] : $payload_data;
if (empty($payloads)) {
    fwrite(STDERR, "Empty payload array.\n");
    exit(1);
}

$payload = $payloads[0];

// --- Load QueueWorker classes ---
// 1. Load plugin's own autoloader (classmap with all QueueWorker classes)
$plugin_autoload = dirname(__DIR__) . '/vendor/autoload.php';
if (file_exists($plugin_autoload)) {
    require_once $plugin_autoload;
}

// 2. Walk up from plugin root to find site autoloader (Bedrock: has Dotenv, etc.)
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
use QueueWorker\Job_Executor;
use QueueWorker\Job_Log;

// --- Discover WordPress and load environment ---
$site_root = $site_autoload ? dirname($site_autoload, 2) : dirname(__DIR__);
Bootstrap::load_dotenv($site_root);
$wp_load = Bootstrap::discover_wp_load(__DIR__);

// --- Bootstrap WordPress with the target site's domain ---
$domain = parse_url($payload['site_url'] ?? '', PHP_URL_HOST);
if (!$domain) {
    $domain = getenv('DOMAIN_CURRENT_SITE') ?: 'localhost';
}

$_SERVER['HTTP_HOST']      = $domain;
$_SERVER['SERVER_NAME']    = $domain;
$_SERVER['REQUEST_URI']    = '/';
$_SERVER['SERVER_PORT']    = '443';
$_SERVER['HTTPS']          = 'on';
$_SERVER['REQUEST_METHOD'] = 'GET';

define('QUEUE_WORKER_RUNNING', true);
if (!defined('DOING_CRON')) {
    define('DOING_CRON', true);
}

require_once $wp_load;

// The payload URL must bootstrap the expected blog. Switching only after
// WordPress loads is unsafe because site-active plugins and database routing
// have already been selected for the wrong site.
$site_id = (int) ($payload['site_id'] ?? get_current_blog_id());
$is_sovereign_tenant = (defined('WU_MT_SOVEREIGN_TENANT') && (int) WU_MT_SOVEREIGN_TENANT === $site_id)
    || qw_payload_matches_sovereign_registry($site_id, $domain);
if (!$is_sovereign_tenant && $site_id !== get_current_blog_id()) {
    fwrite(STDERR, sprintf(
        "Payload site mismatch: URL resolved to blog %d, expected blog %d.\n",
        get_current_blog_id(),
        $site_id
    ));
    exit(1);
}

Job_Log::ensure_table();

// --- Execute jobs ---
exit((new Job_Executor($site_id))->run($payloads));

function qw_payload_matches_sovereign_registry(int $site_id, string $domain): bool
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
