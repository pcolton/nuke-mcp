"""Tests for event system."""

from nukemcp.events import EventLog, EVENT_TYPES


def test_subscribe_all():
    log = EventLog()
    log.subscribe()
    assert log.subscriptions == set(EVENT_TYPES)


def test_subscribe_specific():
    log = EventLog()
    log.subscribe(["node_created", "node_deleted"])
    assert log.subscriptions == {"node_created", "node_deleted"}


def test_subscribe_filters_invalid():
    log = EventLog()
    log.subscribe(["node_created", "fake_event"])
    assert log.subscriptions == {"node_created"}


def test_add_event():
    log = EventLog()
    log.subscribe(["node_created"])
    log.add({"event_type": "node_created", "data": {"name": "Grade1"}})
    assert len(log.get_recent()) == 1
    assert log.get_recent()[0]["data"]["name"] == "Grade1"


def test_add_unsubscribed_event():
    log = EventLog()
    log.subscribe(["node_deleted"])
    log.add({"event_type": "node_created", "data": {}})
    assert len(log.get_recent()) == 0


def test_max_events():
    log = EventLog(max_events=5)
    log.subscribe()
    for i in range(10):
        log.add({"event_type": "node_created", "data": {"i": i}})
    assert len(log.events) == 5
    assert log.events[0]["data"]["i"] == 5  # Oldest retained


def test_clear():
    log = EventLog()
    log.subscribe()
    log.add({"event_type": "node_created", "data": {}})
    log.clear()
    assert len(log.events) == 0


def test_subscribe_events_mock(connection):
    """Test the subscribe_events addon command via mock."""
    response = connection.send({
        "type": "subscribe_events",
        "params": {"event_types": ["node_created", "node_deleted"]},
    })
    assert response["status"] == "ok"
    assert "node_created" in response["result"]["subscribed"]


def test_event_push_received(mock_server, mock_port):
    """Test that pushed events are received by the connection's event handler."""
    import time
    from nukemcp.connection import NukeConnection

    received = []

    conn = NukeConnection("127.0.0.1", mock_port)
    conn.set_event_handler(lambda msg: received.append(msg))
    conn.connect()

    # Push an event from the mock server
    mock_server.push_event("node_created", {"name": "Grade1", "class": "Grade"})

    # Give the reader thread time to process
    for _ in range(50):
        if received:
            break
        time.sleep(0.02)

    assert len(received) == 1
    assert received[0]["type"] == "event"
    assert received[0]["event_type"] == "node_created"
    assert received[0]["data"]["name"] == "Grade1"

    conn.disconnect()


def test_resubscribe_restores_subscriptions(connection, mock_server):
    from nukemcp.events import resubscribe

    log = EventLog()
    log.subscribe(["node_created", "node_deleted"])
    mock_server.state._subscribed_events = []  # a restarted Nuke has none

    resubscribe(connection, log)

    assert set(mock_server.state._subscribed_events) == {"node_created", "node_deleted"}


def test_resubscribe_sends_nothing_without_subscriptions(connection, mock_server):
    from nukemcp.events import resubscribe

    mock_server.state._subscribed_events = ["untouched"]
    resubscribe(connection, EventLog())
    assert mock_server.state._subscribed_events == ["untouched"]


def test_server_resubscribes_after_reconnect(monkeypatch):
    """The built server restores event subscriptions when Nuke comes back."""
    import socket
    import time

    from nukemcp import server as server_module

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    # build_server returns only the FastMCP object, so catch the server it builds
    built = {}

    class Capturing(server_module.NukeMCPServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            built["server"] = self

    monkeypatch.setattr(server_module, "NukeMCPServer", Capturing)
    server_module.build_server(port=port, mock=True)

    server = built["server"]
    conn, mock = server.connection, server._mock_server
    try:
        server.event_log.subscribe(["node_created"])
        conn.send_command("subscribe_events", {"event_types": ["node_created"]})

        mock.stop()
        mock._thread.join(timeout=5.0)
        deadline = time.monotonic() + 5.0
        while conn.is_connected and time.monotonic() < deadline:
            time.sleep(0.01)
        mock.state._subscribed_events = []
        mock.start()

        conn.send_command("ping")
        assert mock.state._subscribed_events == ["node_created"]
    finally:
        conn.disconnect()
        mock.stop()
