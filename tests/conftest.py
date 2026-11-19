"""Shared test fixtures for NukeMCP tests."""

import pytest

from nukemcp.connection import NukeConnection
from nukemcp.mock import MockNukeServer


def _find_free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def mock_port():
    return _find_free_port()


@pytest.fixture
def mock_server(mock_port):
    server = MockNukeServer(port=mock_port)
    server.start()  # Blocks until socket is bound (threading.Event)
    yield server
    server.stop()


@pytest.fixture
def connection(mock_server, mock_port):
    conn = NukeConnection("127.0.0.1", mock_port)
    conn.connect()
    yield conn
    conn.disconnect()


@pytest.fixture
def quit_nuke(mock_server, connection):
    """Return a function that stops the mock, as when Nuke quits.

    It returns once the connection has noticed. Call mock_server.start() afterwards
    to bring "Nuke" back on the same port.
    """
    import time

    def quit_nuke():
        mock_server.stop()
        mock_server._thread.join(timeout=5.0)
        deadline = time.monotonic() + 5.0
        while connection.is_connected and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not connection.is_connected

    return quit_nuke
