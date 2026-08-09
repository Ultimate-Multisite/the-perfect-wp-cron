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
use QueueWorker\Config;
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

$scheduling_horizon = max(1, (int) ($payload['scheduling_horizon'] ?? Config::scheduling_horizon()));
$scan_timeout = max(1, (int) ($payload['scan_timeout'] ?? Config::scan_timeout()));
$payloads = [];
$isolated_network_id = (int) ($payload['isolated_network_id'] ?? 0);
if (!empty($payload['full_network'])) {
    $payloads = qw_scan_full_network_jobs($scheduling_horizon, $scan_timeout);
} elseif ($isolated_network_id > 0) {
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

        $payloads = array_merge($payloads, qw_scan_current_site_jobs($scheduling_horizon));

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

    $payloads = qw_scan_current_site_jobs($scheduling_horizon);
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

function qw_scan_full_network_jobs(int $scheduling_horizon, int $scan_timeout): array
{
    $payloads = [];
    $sovereign_sites = qw_scan_sovereign_site_entries();
    $isolated_networks = qw_scan_isolated_network_entries();
    $initial_blog_id = get_current_blog_id();
    $site_ids = is_multisite() ? get_sites(['number' => 0, 'fields' => 'ids']) : [$initial_blog_id];

    foreach ($site_ids as $site_id) {
        $site_id = (int) $site_id;
        if (isset($sovereign_sites[$site_id])) {
            continue;
        }

        $site = get_site($site_id);
        $network_id = $site ? (int) $site->site_id : 0;
        if (isset($isolated_networks[$network_id])) {
            continue;
        }

        $switched = $site_id !== get_current_blog_id();
        if ($switched) {
            switch_to_blog($site_id);
        }
        $payloads = array_merge($payloads, qw_scan_current_site_jobs($scheduling_horizon));
        if ($switched) {
            restore_current_blog();
        }
    }

    if ($initial_blog_id !== get_current_blog_id()) {
        switch_to_blog($initial_blog_id);
    }

    foreach ($sovereign_sites as $site_id => $entry) {
        $payloads = array_merge($payloads, qw_scan_registry_jobs([
            'site_id'             => (int) $site_id,
            'site_url'            => qw_scan_site_url_from_registry_entry($entry),
            'scheduling_horizon'  => $scheduling_horizon,
            'scan_timeout'        => $scan_timeout,
        ], 'sovereign site ' . $site_id, $scan_timeout));
    }

    foreach ($isolated_networks as $network_id => $entry) {
        $payloads = array_merge($payloads, qw_scan_registry_jobs([
            'isolated_network_id' => (int) $network_id,
            'site_url'            => qw_scan_site_url_from_registry_entry($entry),
            'scheduling_horizon'  => $scheduling_horizon,
            'scan_timeout'        => $scan_timeout,
        ], 'isolated network ' . $network_id, $scan_timeout));
    }

    return $payloads;
}

function qw_scan_current_site_jobs(int $scheduling_horizon): array
{
    wp_cache_delete('cron', 'options');
    wp_cache_delete('alloptions', 'options');

    $payloads = [];
    $deadline = time() + $scheduling_horizon;
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
                    if ((int) $timestamp > $deadline) {
                        continue;
                    }
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
                if (!$action_payload || $action_payload->timestamp > $deadline) {
                    continue;
                }

                $payloads[] = json_decode($action_payload->to_json(), true);
            }
        } catch (\Throwable $e) {
            fwrite(STDERR, "Action Scheduler scan failed: " . $e->getMessage() . "\n");
        }
    }

    return $payloads;
}

function qw_scan_sovereign_site_entries(): array
{
    if (!defined('WP_CONTENT_DIR')) {
        return [];
    }

    $data = qw_scan_registry_data(WP_CONTENT_DIR . '/site-registry.data.json');
    if (empty($data['sites']) || !is_array($data['sites'])) {
        return [];
    }

    $entries = [];
    foreach ($data['sites'] as $site_id => $entry) {
        if (!is_array($entry)
            || ($entry['isolation_model'] ?? '') !== 'sovereign'
            || ($entry['status'] ?? 'active') !== 'active'
            || qw_scan_site_url_from_registry_entry($entry) === ''
        ) {
            continue;
        }
        $entries[(int) $site_id] = $entry;
    }

    return $entries;
}

function qw_scan_isolated_network_entries(): array
{
    if (!defined('WP_CONTENT_DIR')) {
        return [];
    }

    $data = qw_scan_registry_data(WP_CONTENT_DIR . '/network-registry.data.json');
    if (empty($data['networks']) || !is_array($data['networks'])) {
        return [];
    }

    $entries = [];
    foreach ($data['networks'] as $registry_id => $entry) {
        if (!is_array($entry)
            || ($entry['tier'] ?? '') !== 'isolated'
            || ($entry['status'] ?? 'active') !== 'active'
            || qw_scan_site_url_from_registry_entry($entry) === ''
        ) {
            continue;
        }

        $network_id = (int) ($entry['network_id'] ?? $entry['id'] ?? $registry_id);
        if ($network_id > 0) {
            $entries[$network_id] = $entry;
        }
    }

    return $entries;
}

function qw_scan_registry_data(string $path): array
{
    if (!is_readable($path)) {
        return [];
    }

    $data = json_decode((string) file_get_contents($path), true);
    return is_array($data) ? $data : [];
}

function qw_scan_site_url_from_registry_entry(array $entry): string
{
    $domains = $entry['domains'] ?? [];
    if (!is_array($domains)) {
        $domains = [];
    }
    if (!empty($entry['domain'])) {
        array_unshift($domains, $entry['domain']);
    }

    foreach ($domains as $domain) {
        $domain = trim((string) $domain);
        if ($domain !== '') {
            return 'https://' . $domain . '/';
        }
    }

    return '';
}

function qw_scan_registry_jobs(array $payload, string $description, int $scan_timeout): array
{
    $process = proc_open([PHP_BINARY, __FILE__, '--stdin'], [
        0 => ['pipe', 'r'],
        1 => ['pipe', 'w'],
        2 => ['pipe', 'w'],
    ], $pipes);
    if (!is_resource($process)) {
        fwrite(STDERR, sprintf("Could not start scanner for %s.\n", $description));
        return [];
    }

    $encoded_payload = json_encode($payload);
    if ($encoded_payload === false || fwrite($pipes[0], $encoded_payload) === false) {
        fclose($pipes[0]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        proc_terminate($process, 9);
        proc_close($process);
        fwrite(STDERR, sprintf("Could not configure scanner for %s.\n", $description));
        return [];
    }

    fclose($pipes[0]);
    stream_set_blocking($pipes[1], false);
    stream_set_blocking($pipes[2], false);
    $stdout = '';
    $stderr = '';
    $started = time();
    do {
        $out = stream_get_contents($pipes[1]);
        if ($out !== false && $out !== '') {
            $stdout .= $out;
        }
        $err = stream_get_contents($pipes[2]);
        if ($err !== false && $err !== '') {
            $stderr .= $err;
        }

        $status = proc_get_status($process);
        if (!$status['running']) {
            break;
        }
        if (time() - $started > $scan_timeout) {
            proc_terminate($process, 9);
            fwrite(STDERR, sprintf("Scanner for %s exceeded %d seconds.\n", $description, $scan_timeout));
            break;
        }
        usleep(10000);
    } while (true);

    $remaining = stream_get_contents($pipes[1]);
    if ($remaining !== false && $remaining !== '') {
        $stdout .= $remaining;
    }
    $remaining_error = stream_get_contents($pipes[2]);
    if ($remaining_error !== false && $remaining_error !== '') {
        $stderr .= $remaining_error;
    }
    fclose($pipes[1]);
    fclose($pipes[2]);
    $close_code = proc_close($process);
    $exit_code = (int) ($status['exitcode'] ?? $close_code);
    if ($exit_code === -1) {
        $exit_code = $close_code;
    }

    if ($exit_code !== 0 || $stdout === '') {
        fwrite(STDERR, sprintf(
            "Scanner for %s failed (exit %d, stdout_bytes=%d, stderr_bytes=%d).\n",
            $description,
            $exit_code,
            strlen($stdout),
            strlen($stderr)
        ));
        return [];
    }

    try {
        $jobs = json_decode($stdout, true, 512, JSON_THROW_ON_ERROR);
    } catch (\JsonException $e) {
        fwrite(STDERR, sprintf("Scanner for %s returned invalid JSON.\n", $description));
        return [];
    }

    if (!array_is_list($jobs)) {
        fwrite(STDERR, sprintf("Scanner for %s returned an invalid payload shape.\n", $description));
        return [];
    }

    foreach ($jobs as $job) {
        if (!is_array($job)) {
            fwrite(STDERR, sprintf("Scanner for %s returned an invalid payload shape.\n", $description));
            return [];
        }
    }

    return $jobs;
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
