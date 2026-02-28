<?php
/**
 * Execute one or more jobs in a fresh WordPress environment.
 *
 * Spawned by worker.php as a subprocess. Bootstraps WordPress with the correct
 * site domain so all per-site plugins are loaded and DB tables are correct.
 * Accepts a single payload (legacy) or an array of payloads for batch execution.
 * All payloads in a batch must share the same site_id and site_url.
 *
 * Usage:
 *   php execute-job.php <base64-encoded-json-payload>
 *
 * Exit codes:
 *   0 = all jobs succeeded
 *   1 = one or more jobs failed
 */

// Payload comes as base64-encoded JSON argument
$raw = base64_decode($argv[1] ?? '', true);
if (!$raw) {
    fwrite(STDERR, "Missing or invalid base64 payload argument.\n");
    exit(1);
}
$payload_data = json_decode($raw, true);
if (!is_array($payload_data)) {
    fwrite(STDERR, "Invalid JSON payload.\n");
    exit(1);
}

// Normalize: single payload (has 'hook' key) → wrap in array
$payloads = isset($payload_data['hook']) ? [$payload_data] : $payload_data;
if (empty($payloads)) {
    fwrite(STDERR, "Empty payload array.\n");
    exit(1);
}

// Use first payload for site context (all share same site)
$payload = $payloads[0];

// --- Auto-discover vendor/autoload.php ---
$autoload = null;
$search = __DIR__;
for ($i = 0; $i < 10; $i++) {
    $search = dirname($search);
    if (file_exists($search . '/vendor/autoload.php')) {
        $autoload = $search . '/vendor/autoload.php';
        break;
    }
}
if (!$autoload) {
    fwrite(STDERR, "Could not find vendor/autoload.php.\n");
    exit(1);
}
require_once $autoload;

// --- Auto-discover wp-load.php ---
$wp_load = null;
if (getenv('WP_ROOT_PATH')) {
    $root = rtrim(getenv('WP_ROOT_PATH'), '/');
    if (file_exists($root . '/wp-load.php')) {
        $wp_load = $root . '/wp-load.php';
    }
}
if (!$wp_load) {
    $search = dirname(__DIR__);
    for ($i = 0; $i < 10; $i++) {
        $search = dirname($search);
        if (file_exists($search . '/wp-load.php')) {
            $wp_load = $search . '/wp-load.php';
            break;
        }
        if (file_exists($search . '/web/wp/wp-load.php')) {
            $wp_load = $search . '/web/wp/wp-load.php';
            break;
        }
    }
}
if (!$wp_load) {
    fwrite(STDERR, "Could not find wp-load.php.\n");
    exit(1);
}

// Load .env if available (Bedrock)
$site_root = dirname($autoload, 2);
if (class_exists('Dotenv\\Dotenv') && file_exists($site_root . '/.env')) {
    $env_files = file_exists($site_root . '/.env.local')
        ? ['.env', '.env.local']
        : ['.env'];
    $dotenv = \Dotenv\Dotenv::createUnsafeImmutable($site_root, $env_files, false);
    $dotenv->load();
}

// --- Bootstrap WordPress with the target site's domain ---
$domain = parse_url($payload['site_url'] ?? '', PHP_URL_HOST);
if (!$domain) {
    // Fallback to DOMAIN_CURRENT_SITE
    $domain = getenv('DOMAIN_CURRENT_SITE') ?: 'localhost';
}

$_SERVER['HTTP_HOST']      = $domain;
$_SERVER['SERVER_NAME']    = $domain;
$_SERVER['REQUEST_URI']    = '/';
$_SERVER['SERVER_PORT']    = '443';
$_SERVER['HTTPS']          = 'on';
$_SERVER['REQUEST_METHOD'] = 'GET';

define('QUEUE_WORKER_RUNNING', true);

require_once $wp_load;

// Ensure we're on the correct blog (safety net)
$site_id = (int) ($payload['site_id'] ?? get_current_blog_id());
if ($site_id !== get_current_blog_id()) {
    switch_to_blog($site_id);
}

// --- Execute jobs ---
$exit_code = 0;
$results   = [];

foreach ($payloads as $i => $job) {
    $source = $job['source'] ?? 'wp_cron';
    $hook   = $job['hook'];

    try {
        if ($source === 'action_scheduler') {
            $action_id = (int) ($job['action_id'] ?? 0);
            if (!$action_id || !class_exists('ActionScheduler_QueueRunner')) {
                throw new \RuntimeException('ActionScheduler not available or missing action_id');
            }
            $runner = \ActionScheduler_QueueRunner::instance();
            $runner->process_action($action_id);
            $results[] = ['status' => 'ok', 'type' => 'as', 'action_id' => $action_id, 'hook' => $hook];
        } else {
            $timestamp = (int) ($job['timestamp'] ?? 0);
            $args      = $job['args'] ?? [];
            wp_unschedule_event($timestamp, $hook, $args);
            do_action_ref_array($hook, $args);
            $results[] = ['status' => 'ok', 'type' => 'cron', 'hook' => $hook];
        }
    } catch (\Throwable $e) {
        fwrite(STDERR, "[Job $i] {$hook}: " . $e->getMessage() . "\n");
        $results[] = ['status' => 'error', 'hook' => $hook, 'error' => $e->getMessage()];
        $exit_code = 1;
    }
}

echo json_encode($results);
exit($exit_code);
