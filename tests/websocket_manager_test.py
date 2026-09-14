import json
import time
from unittest.mock import MagicMock

import pytest
import websocket

from hyperliquid.utils.types import Fill, Subscription
from hyperliquid.websocket_manager import WebsocketManager, subscription_to_identifier, ws_msg_to_identifier

MIXED_CASE_USER = "0xAF0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
FILL: Fill = {
    "coin": "BTC",
    "side": "A",
    "px": "79000",
    "sz": "1",
    "time": 1788819919000,
    "startPosition": "-3",
    "dir": "Open Short",
    "closedPnl": "0",
    "hash": "0x...",
    "oid": 1,
    "crossed": True,
    "fee": "0",
    "tid": 1,
    "feeToken": "USDC",
}
FILLS_SUBSCRIPTION: Subscription = {"type": "userFills", "user": MIXED_CASE_USER}
TWAP_SLICE_FILLS_SUBSCRIPTION: Subscription = {"type": "userTwapSliceFills", "user": MIXED_CASE_USER}
FILLS_MSG = {"channel": "userFills", "data": {"user": MIXED_CASE_USER, "isSnapshot": False, "fills": [FILL]}}


def get_connected_manager() -> tuple[WebsocketManager, MagicMock]:
    manager = WebsocketManager("https://api.hyperliquid.xyz")
    ws = MagicMock()
    manager.ws = ws
    manager.ws_ready = True
    return manager, ws


def get_sent_subscriptions(ws: MagicMock, method: str = "subscribe") -> list[Subscription]:
    frames = [json.loads(call.args[0]) for call in ws.send.call_args_list]
    return [frame["subscription"] for frame in frames if frame["method"] == method]


@pytest.mark.parametrize(
    "subscription, ws_msg, expected_identifier",
    [
        (FILLS_SUBSCRIPTION, FILLS_MSG, f"userFills:{MIXED_CASE_USER.lower()}"),
        (
            TWAP_SLICE_FILLS_SUBSCRIPTION,
            {
                "channel": "userTwapSliceFills",
                "data": {"user": MIXED_CASE_USER, "twapSliceFills": [{"twapId": 2194823, "fill": FILL}]},
            },
            f"userTwapSliceFills:{MIXED_CASE_USER.lower()}",
        ),
    ],
)
def test_user_keyed_subscription_and_message_share_identifier(subscription, ws_msg, expected_identifier):
    assert subscription_to_identifier(subscription) == expected_identifier
    assert ws_msg_to_identifier(ws_msg) == expected_identifier


def test_subscribe_sends_one_frame_per_identifier_and_fans_out_callbacks():
    manager, ws = get_connected_manager()
    first_callback, second_callback = MagicMock(), MagicMock()

    first_subscription_id = manager.subscribe(FILLS_SUBSCRIPTION, first_callback)
    second_subscription_id = manager.subscribe(FILLS_SUBSCRIPTION, second_callback)
    manager.on_message(ws, json.dumps(FILLS_MSG))

    assert get_sent_subscriptions(ws) == [FILLS_SUBSCRIPTION]
    first_callback.assert_called_once_with(FILLS_MSG)
    second_callback.assert_called_once_with(FILLS_MSG)

    assert not manager.unsubscribe(TWAP_SLICE_FILLS_SUBSCRIPTION, first_subscription_id)
    assert manager.unsubscribe(FILLS_SUBSCRIPTION, first_subscription_id)
    assert get_sent_subscriptions(ws, method="unsubscribe") == []
    assert manager.unsubscribe(FILLS_SUBSCRIPTION, second_subscription_id)
    assert get_sent_subscriptions(ws, method="unsubscribe") == [FILLS_SUBSCRIPTION]


def test_on_open_replays_active_and_queued_subscriptions_once_per_identifier():
    manager, ws = get_connected_manager()
    first_subscription_id = manager.subscribe(FILLS_SUBSCRIPTION, MagicMock())
    manager.subscribe(FILLS_SUBSCRIPTION, MagicMock())
    manager.on_close(ws, 1006, None)
    manager.subscribe(TWAP_SLICE_FILLS_SUBSCRIPTION, MagicMock())
    queued_subscription_id = manager.subscribe(FILLS_SUBSCRIPTION, MagicMock())
    assert manager.unsubscribe(FILLS_SUBSCRIPTION, first_subscription_id)
    assert manager.unsubscribe(FILLS_SUBSCRIPTION, queued_subscription_id)
    ws.send.reset_mock()

    manager.on_open(ws)

    assert get_sent_subscriptions(ws) == [FILLS_SUBSCRIPTION, TWAP_SLICE_FILLS_SUBSCRIPTION]
    assert len(manager.active_subscriptions[subscription_to_identifier(FILLS_SUBSCRIPTION)]) == 1
    assert not manager.queued_subscriptions
    assert manager.ws_ready


@pytest.mark.parametrize(
    "subscription", [{"type": "userEvents", "user": MIXED_CASE_USER}, {"type": "orderUpdates", "user": MIXED_CASE_USER}]
)
def test_subscribe_rejects_second_unmultiplexed_subscription_before_open(subscription):
    manager, ws = get_connected_manager()
    manager.on_close(ws, 1006, None)
    manager.subscribe(subscription, MagicMock())

    with pytest.raises(NotImplementedError):
        manager.subscribe(subscription, MagicMock())

    manager.on_open(ws)
    assert get_sent_subscriptions(ws) == [subscription]
    assert manager.ws_ready


def test_subscribe_on_dropped_socket_defers_to_replay_on_open():
    manager, ws = get_connected_manager()
    ws.send.side_effect = websocket.WebSocketConnectionClosedException("Connection is already closed.")

    manager.subscribe(FILLS_SUBSCRIPTION, MagicMock())

    assert not manager.ws_ready
    ws.send.side_effect = None
    ws.send.reset_mock()
    manager.on_open(ws)
    assert get_sent_subscriptions(ws) == [FILLS_SUBSCRIPTION]
    assert manager.ws_ready


def test_run_restarts_pong_window_before_connecting():
    manager, ws = get_connected_manager()
    manager.last_pong_time = 0.0
    ws.run_forever.side_effect = manager.stop_event.set

    manager.run()

    assert time.time() - manager.last_pong_time < manager.heartbeat_interval


def test_error_message_keeps_websocket_running():
    manager, ws = get_connected_manager()

    manager.on_message(ws, json.dumps({"channel": "error", "data": "Already subscribed"}))

    assert not manager.stop_event.is_set()
