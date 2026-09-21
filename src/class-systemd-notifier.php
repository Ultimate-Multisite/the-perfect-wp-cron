<?php

namespace QueueWorker;

/** Optional systemd coordinator liveness notification (no shell subprocess). */
class Systemd_Notifier
{
    public static function heartbeat(): bool
    {
        $path = getenv('NOTIFY_SOCKET');
        if ($path === false || $path === '') {
            return true;
        }
        if (!function_exists('socket_create') || !in_array($path[0], ['/', '@'], true)) {
            return false;
        }
        $socket = @socket_create(AF_UNIX, SOCK_DGRAM, 0);
        if ($socket === false) {
            return false;
        }
        try {
            // systemd uses @ to represent Linux's abstract socket namespace.
            $address = $path[0] === '@' ? "\0" . substr($path, 1) : $path;
            $message = 'WATCHDOG=1';
            socket_set_nonblock($socket);
            // connect()+send() supports abstract AF_UNIX addresses on PHP,
            // whereas sendto() can truncate their leading NUL (EINVAL).
            return @socket_connect($socket, $address)
                && @socket_send($socket, $message, strlen($message), 0) === strlen($message);
        } finally {
            socket_close($socket);
        }
    }
}
