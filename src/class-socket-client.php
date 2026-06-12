<?php

namespace QueueWorker;

class Socket_Client
{
    private static string $last_error = '';

    public static function get_socket_path(): string
    {
        return Config::socket_path();
    }

    public static function get_last_error(): string
    {
        return self::$last_error;
    }

    public static function notify(Job_Payload $payload): bool
    {
        $path = self::get_socket_path();
        $socket = @stream_socket_client('unix://' . $path, $errno, $errstr, 1);
        if (!$socket) {
            self::log_notify_failure($payload, sprintf(
                'connect failed for %s: [%d] %s',
                $path,
                (int) $errno,
                $errstr !== '' ? $errstr : 'unknown error'
            ));
            return false;
        }

        $message = $payload->to_json() . "\n";
        $written = fwrite($socket, $message);
        fclose($socket);

        if ($written !== strlen($message)) {
            self::log_notify_failure($payload, sprintf(
                'write failed for %s: wrote %d of %d bytes',
                $path,
                $written === false ? 0 : $written,
                strlen($message)
            ));
            return false;
        }

        return true;
    }

    private static function log_notify_failure(Job_Payload $payload, string $reason): void
    {
        error_log(sprintf(
            '[The Perfect WP Cron][Socket Notify Failed] source=%s site_id=%d hook=%s action_id=%d reason=%s',
            $payload->source,
            $payload->site_id,
            $payload->hook,
            $payload->action_id,
            $reason
        ));
    }

    public static function send_command(string $command, int $timeout = 5): ?array
    {
        self::$last_error = '';
        $path = self::get_socket_path();
        $socket = @stream_socket_client('unix://' . $path, $errno, $errstr, 2);
        if (!$socket) {
            self::$last_error = sprintf('connect_failed: %s (%d)', $errstr ?: 'unknown error', $errno);
            return null;
        }

        fwrite($socket, json_encode(['command' => $command]) . "\n");
        stream_set_timeout($socket, $timeout);
        $response = fgets($socket);
        $meta = stream_get_meta_data($socket);
        fclose($socket);

        if (!$response) {
            self::$last_error = !empty($meta['timed_out']) ? 'timeout_waiting_for_response' : 'empty_response';
            return null;
        }

        $data = json_decode($response, true);
        if (!is_array($data)) {
            self::$last_error = 'malformed_response';
            return null;
        }

        return $data;
    }

    public static function is_worker_running(): bool
    {
        return file_exists(self::get_socket_path());
    }
}
