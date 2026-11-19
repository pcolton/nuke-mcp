"""Tests for NukeConnection.send_command and error handling."""

import pytest

from nukemcp.connection import NukeConnectionError


def test_send_command_success(connection):
    result = connection.send_command("get_script_info")
    assert "name" in result
    assert "fps" in result


def test_send_command_error_raises(connection):
    with pytest.raises(RuntimeError, match="not found"):
        connection.send_command("get_node_info", {"node_name": "nonexistent"})


def test_send_command_unknown_command(connection):
    with pytest.raises(RuntimeError, match="Unknown command"):
        connection.send_command("totally_bogus_command")


def test_send_command_with_params(connection):
    result = connection.send_command("create_node", {"node_class": "Grade", "name": "MyGrade"})
    assert result["name"] == "MyGrade"
    assert result["class"] == "Grade"


def test_send_not_connected():
    from nukemcp.connection import NukeConnection
    conn = NukeConnection()
    with pytest.raises(NukeConnectionError, match="Not connected"):
        conn.send({"type": "ping"})


def test_reconnects_after_nuke_restarts(connection, mock_server, quit_nuke):
    quit_nuke()
    mock_server.start()
    assert connection.send_command("ping") == "pong"
    assert connection.is_connected


def test_reconnect_fails_while_nuke_is_down(connection, mock_server, quit_nuke):
    quit_nuke()
    with pytest.raises(NukeConnectionError, match="could not reconnect"):
        connection.send_command("ping")
    # Each command tries again, so it recovers once Nuke is back
    mock_server.start()
    assert connection.send_command("ping") == "pong"


def test_reconnects_when_send_fails(connection):
    """The peer has gone but the reader has not noticed, so the send itself fails."""

    class FailingSend:
        def __init__(self, sock):
            self._sock = sock

        def sendall(self, data):
            raise BrokenPipeError("peer gone")

        def shutdown(self, how):
            self._sock.shutdown(how)

        def close(self):
            self._sock.close()

    connection.sock = FailingSend(connection.sock)
    assert connection.send_command("ping") == "pong"


def test_no_reconnect_after_disconnect(connection):
    connection.disconnect()
    with pytest.raises(NukeConnectionError, match="Not connected"):
        connection.send({"type": "ping"})


def test_reconnect_handlers_run_after_reconnect(connection, mock_server, quit_nuke):
    calls = []
    connection.add_reconnect_handler(lambda: calls.append(connection.send_command("ping")))
    connection.send_command("ping")
    assert calls == []

    quit_nuke()
    mock_server.start()
    connection.send_command("ping")
    assert calls == ["pong"]


def test_failing_reconnect_handler_does_not_break_the_command(connection, mock_server, quit_nuke):
    def broken():
        raise ValueError("handler bug")

    connection.add_reconnect_handler(broken)
    quit_nuke()
    mock_server.start()
    assert connection.send_command("ping") == "pong"
