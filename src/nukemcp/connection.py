"""Socket connection to the Nuke addon."""

import json
import logging
import queue
import socket
import threading
import time

log = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 54321
RECV_BUFFER = 1024 * 1024  # 1MB


class NukeConnectionError(Exception):
    """Failed to connect or communicate with the Nuke addon."""


class NukeConnection:
    """Manages a TCP socket connection to the Nuke addon running inside Nuke.

    On connect, reads a handshake message containing Nuke version and variant info.
    Sends JSON commands and receives JSON responses. Supports receiving asynchronous
    event messages pushed by the addon.

    If the addon drops the connection, for example because Nuke was restarted, the
    next command reconnects first. An explicit disconnect() turns that off.
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        self.host = host
        self.port = port
        self.sock: socket.socket | None = None
        self.handshake: dict | None = None
        self._buffer = b""
        self._response_queue: queue.Queue[dict] = queue.Queue()
        self._event_handler: callable | None = None
        self._reader_thread: threading.Thread | None = None
        self._reader_running = False
        self._auto_reconnect = False
        self._reconnect_lock = threading.Lock()
        self._reconnect_handlers: list[callable] = []

    def set_event_handler(self, handler):
        """Set a callback for event messages pushed by the addon.

        The handler receives a dict with keys: type, event_type, data.
        """
        self._event_handler = handler

    def add_reconnect_handler(self, handler):
        """Add a callback to run after each automatic reconnect.

        A restarted Nuke has lost whatever the addon was told before, such as
        event subscriptions. The handler takes no arguments and can send
        commands to restore that state.
        """
        self._reconnect_handlers.append(handler)

    def connect(self, retries: int = 3, base_delay: float = 1.0) -> dict:
        """Connect to the Nuke addon and return the handshake data.

        Retries with exponential backoff on failure.
        """
        last_error = None
        for attempt in range(retries):
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10.0)
                sock.connect((self.host, self.port))
                self._buffer = b""
                self.handshake = self._read_message(sock)
                log.info(
                    "Connected to Nuke %s (%s) at %s:%d",
                    self.handshake.get("nuke_version", "unknown"),
                    self.handshake.get("variant", "unknown"),
                    self.host,
                    self.port,
                )
                # Responses still queued belong to an earlier connection
                while not self._response_queue.empty():
                    self._response_queue.get_nowait()
                # Switch to blocking mode and start the reader thread
                sock.settimeout(None)
                self.sock = sock
                self._auto_reconnect = True
                self._start_reader(sock)
                return self.handshake
            except (OSError, json.JSONDecodeError, TimeoutError) as e:
                last_error = e
                if sock is not None:
                    self._close_socket(sock)
                if attempt < retries - 1:
                    delay = base_delay * (2**attempt)
                    log.warning(
                        "Connection attempt %d failed: %s. Retrying in %.1fs...",
                        attempt + 1,
                        e,
                        delay,
                    )
                    time.sleep(delay)

        raise NukeConnectionError(
            f"Failed to connect to Nuke addon at {self.host}:{self.port} "
            f"after {retries} attempts: {last_error}"
        )

    def send(self, command: dict, timeout: float = 10.0) -> dict:
        """Send a JSON command and return the JSON response.

        Reconnects first if the addon dropped the connection since the last command.
        """
        data = (json.dumps(command) + "\n").encode("utf-8")
        sock = self.sock or self._reconnect()
        try:
            sock.sendall(data)
        except OSError:
            # The addon went away and the reader has not noticed yet. The command
            # did not reach it, so it is safe to send again on a new connection.
            self._close_socket(sock)
            sock = self._reconnect()
            try:
                sock.sendall(data)
            except OSError as e:
                self._close_socket(sock)
                raise NukeConnectionError(f"Communication error: {e}") from e

        try:
            return self._response_queue.get(timeout=timeout)
        except queue.Empty:
            raise NukeConnectionError(
                f"Timeout waiting for response after {timeout}s"
            ) from None

    def send_command(self, cmd_type: str, params: dict | None = None, timeout: float = 10.0) -> dict:
        """Send a command and return the result, raising on error."""
        response = self.send({"type": cmd_type, "params": params or {}}, timeout=timeout)
        if response["status"] == "error":
            raise RuntimeError(response["error"])
        return response["result"]

    def disconnect(self):
        """Cleanly disconnect from the Nuke addon."""
        self._auto_reconnect = False
        # Close the socket before joining the reader, which is blocked reading it
        self._reader_running = False
        self._close_socket()
        self._stop_reader()
        self.handshake = None
        log.info("Disconnected from Nuke addon")

    @property
    def is_connected(self) -> bool:
        return self.sock is not None

    def _reconnect(self) -> socket.socket:
        """Connect again after the addon dropped the connection.

        Returns the new socket. Raises NukeConnectionError if this connection was
        never made or was closed with disconnect(), or if the addon is not there.
        """
        with self._reconnect_lock:
            if self.sock:
                return self.sock  # another thread reconnected first
            if not self._auto_reconnect:
                raise NukeConnectionError("Not connected to Nuke addon")

            previous = self.handshake or {}
            log.info("Lost the connection to the Nuke addon. Reconnecting...")
            try:
                handshake = self.connect(retries=1)
            except NukeConnectionError as e:
                raise NukeConnectionError(
                    "Lost the connection to the Nuke addon and could not reconnect. "
                    f"Check that Nuke is running and the addon is started. ({e})"
                ) from e
            sock = self.sock

        for key in ("nuke_version", "variant"):
            if handshake.get(key) != previous.get(key):
                log.warning(
                    "Reconnected to a different Nuke (%s was %s, now %s). "
                    "The tools on offer still match the earlier one.",
                    key,
                    previous.get(key),
                    handshake.get(key),
                )

        for handler in self._reconnect_handlers:
            try:
                handler()
            except Exception:
                log.exception("Reconnect handler error")
        return sock

    def _start_reader(self, sock: socket.socket):
        """Start the background thread that reads all incoming messages."""
        self._reader_running = True
        self._reader_thread = threading.Thread(
            target=self._reader_loop, args=(sock,), daemon=True
        )
        self._reader_thread.start()

    def _stop_reader(self):
        """Stop the reader thread."""
        self._reader_running = False
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=2.0)
        self._reader_thread = None

    def _reader_loop(self, sock: socket.socket):
        """Background loop: read messages and route events vs responses.

        Ends when its socket fails or is replaced by a newer connection.
        """
        while self._reader_running and self.sock is sock:
            try:
                msg = self._read_message(sock)
            except (NukeConnectionError, OSError):
                # Forget the dead socket so the next command reconnects
                self._close_socket(sock)
                break
            if msg.get("type") == "event":
                if self._event_handler:
                    try:
                        self._event_handler(msg)
                    except Exception:
                        log.exception("Event handler error")
            else:
                self._response_queue.put(msg)

    def _read_message(self, sock: socket.socket) -> dict:
        """Read a newline-delimited JSON message from the socket."""
        while b"\n" not in self._buffer:
            chunk = sock.recv(RECV_BUFFER)
            if not chunk:
                self._close_socket(sock)
                raise NukeConnectionError("Connection closed by Nuke addon")
            self._buffer += chunk

        line, self._buffer = self._buffer.split(b"\n", 1)
        return json.loads(line.decode("utf-8"))

    def _close_socket(self, sock: socket.socket | None = None):
        """Close a socket, by default the current one.

        The connection only forgets its current socket if that is the one being
        closed, so a reader for an old socket cannot drop a newer connection.
        """
        if sock is None:
            sock = self.sock
        if sock is None:
            return
        if self.sock is sock:
            self.sock = None
        try:
            sock.shutdown(socket.SHUT_RDWR)  # wakes a reader blocked in recv
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass
