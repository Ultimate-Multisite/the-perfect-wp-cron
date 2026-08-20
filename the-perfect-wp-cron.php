<?php
/**
 * Plugin Name: The Perfect WP Cron
 * Plugin URI: https://github.com/Ultimate-Multisite/the-perfect-wp-cron
 * Description: Event-loop job queue for WordPress. Executes WP Cron and Action Scheduler jobs at exact scheduled times with zero polling using Workerman.
 * Version: 1.0.2
 * Author: David Stone
 * Author URI: https://ultimatemultisite.com
 * License: GPL-2.0-or-later
 * License URI: https://www.gnu.org/licenses/gpl-2.0.html
 * Requires PHP: 8.1
 * Network: true
 */

if ( ! defined( 'THE_PERFECT_WP_CRON_VERSION' ) ) {
	define( 'THE_PERFECT_WP_CRON_VERSION', '1.0.2' );
}

if (!defined('ABSPATH')) {
    return;
}

define('QW_PLUGIN_DIR', __DIR__);
define('QW_PLUGIN_URL', plugin_dir_url(__FILE__));

// Autoload: Bedrock's site autoloader handles this in most installs.
// For standalone installs, fall back to the plugin's own vendor autoloader.
if (!class_exists('QueueWorker\\Config')) {
    require_once __DIR__ . '/vendor/autoload.php';
}

// The long-running worker and scanner must not intercept their own bootstrap
// activity. Job executor subprocesses do need the interceptors so cron events
// scheduled by callbacks are sent straight back to the parent worker.
$queue_worker_running = defined('QUEUE_WORKER_RUNNING') && QUEUE_WORKER_RUNNING;
$job_executor_running = defined('QUEUE_WORKER_EXECUTOR_RUNNING') && QUEUE_WORKER_EXECUTOR_RUNNING;

if ($queue_worker_running && !$job_executor_running) {
    return;
}

add_filter('wu_wp_cron_status_override', ['QueueWorker\\Socket_Client', 'filter_wp_cron_status_override']);
add_action('init', ['QueueWorker\\Cron_Interceptor', 'register']);
QueueWorker\Action_Scheduler_Bridge::register_stored_action_hook();
add_action('action_scheduler_init', ['QueueWorker\\Action_Scheduler_Bridge', 'register']);

if ($job_executor_running) {
    return;
}

// Admin menu
$menu_hook = is_multisite() ? 'network_admin_menu' : 'admin_menu';
add_action($menu_hook, ['QueueWorker\\Admin_Page', 'register_menu']);

// AJAX handlers (always on admin_init, even on network admin)
add_action('wp_ajax_qw_worker_status', ['QueueWorker\\Admin_Page', 'ajax_worker_status']);

// WP-CLI commands
if (defined('WP_CLI') && WP_CLI) {
    WP_CLI::add_command('queue', 'QueueWorker\\CLI_Commands');
}

// Activation hook: create job log table
register_activation_hook(__FILE__, function () {
    QueueWorker\Job_Log::ensure_table();
});

// Daily cleanup cron
add_action('qw_cleanup_job_log', function () {
    QueueWorker\Job_Log::cleanup();
});

if (!wp_next_scheduled('qw_cleanup_job_log')) {
    wp_schedule_event(time(), 'daily', 'qw_cleanup_job_log');
}
