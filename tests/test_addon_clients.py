"""The Nuke addon serves several MCP servers at once, without Nuke or Qt."""

import importlib.util
import json
import socket
import sys
import time
import types
from pathlib import Path

import pytest

ADDON = Path(__file__).resolve().parents[1] / "nuke_addon" / "nuke_mcp_addon.py"


class _Signal:
    def __init__(self, *args):
        pass

    def emit(self, *args):
        pass

    def connect(self, *args):
        pass


@pytest.fixture
def addon(monkeypatch):
    """The addon module, loaded with stand-ins for PySide6 and a headless nuke."""
    widgets = types.ModuleType("PySide6.QtWidgets")
    for name in ("QWidget", "QVBoxLayout", "QHBoxLayout", "QLabel", "QPushButton", "QTextEdit"):
        setattr(widgets, name, object)
    core = types.ModuleType("PySide6.QtCore")
    core.Qt, core.QObject, core.Signal = object, object, _Signal
    gui = types.ModuleType("PySide6.QtGui")
    gui.QTextCursor = object
    nuke = types.ModuleType("nuke")
    nuke.GUI = False
    nuke.NUKE_VERSION_STRING = "17.1v3"
    nuke.env = {}
    nuke.addOnCreate = lambda callback: None
    modules = {"PySide6": types.ModuleType("PySide6"), "PySide6.QtWidgets": widgets,
               "PySide6.QtCore": core, "PySide6.QtGui": gui, "nuke": nuke}
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("nuke_mcp_addon_under_test", ADDON)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def server(addon, mock_port):
    s = addon.NukeMCPServer(port=mock_port)
    s.start()
    deadline = time.monotonic() + 5
    while s._server_sock is None or s._server_sock.getsockname()[1] != mock_port:
        if time.monotonic() > deadline:
            raise TimeoutError("addon never listened")
        time.sleep(0.02)
    yield s
    s.stop()


class Client:
    """A bare line-JSON client, as nukemcp.connection speaks to the addon."""

    def __init__(self, port):
        self.sock = None
        deadline = time.monotonic() + 5
        while self.sock is None:
            try:
                self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
            except ConnectionRefusedError:  # The listener may not be up yet.
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.02)
        self.file = self.sock.makefile("rb")
        self.handshake = self.read()

    def read(self):
        line = self.file.readline()
        return json.loads(line) if line else None

    def call(self, cmd_type, params=None):
        self.sock.sendall((json.dumps({"type": cmd_type, "params": params or {}}) + "\n").encode())
        return self.read()

    def close(self):
        self.file.close()
        self.sock.close()


def test_two_clients_are_served_at_once(server, mock_port):
    first, second = Client(mock_port), Client(mock_port)  # Before, the second waited for the first to leave.
    assert first.handshake["type"] == second.handshake["type"] == "handshake"
    assert first.call("ping")["result"] == "pong"
    assert second.call("ping")["result"] == "pong"
    assert first.call("ping")["result"] == "pong"


def test_events_go_only_to_clients_that_subscribed(addon, server, mock_port):
    listener, other = Client(mock_port), Client(mock_port)
    assert listener.call("subscribe_events", {"event_types": ["node_created"]})["status"] == "ok"
    addon._push_event("node_created", {"name": "Blur1", "class": "Blur"})
    event = listener.read()
    assert (event["type"], event["event_type"], event["data"]["name"]) == ("event", "node_created", "Blur1")
    # The other client's next line is its own answer, not the event.
    assert other.call("ping") == {"status": "ok", "result": "pong"}


def test_a_client_leaving_leaves_the_others_connected(addon, server, mock_port):
    leaving, staying = Client(mock_port), Client(mock_port)
    leaving.close()
    assert staying.call("ping")["result"] == "pong"
    assert Client(mock_port).call("ping")["result"] == "pong"
    deadline = time.monotonic() + 5
    while len(addon._clients) != 2 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert len(addon._clients) == 2


def test_stop_disconnects_every_client(addon, server, mock_port):
    clients = [Client(mock_port), Client(mock_port)]
    server.stop()
    for c in clients:
        assert c.read() is None  # End of stream, not a hang.
