import json
from unittest.mock import MagicMock

import pytest

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


def get_sent_subscriptions(ws: MagicMock) -> list[Subscription]:
    frames = [json.loads(call.args[0]) for call in ws.send.call_args_list]
    return [frame["subscription"] for frame in frames if frame["method"] == "subscribe"]


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

    manager.subscribe(FILLS_SUBSCRIPTION, first_callback)
    manager.subscribe(FILLS_SUBSCRIPTION, second_callback)
    manager.on_message(ws, json.dumps(FILLS_MSG))

    assert get_sent_subscriptions(ws) == [FILLS_SUBSCRIPTION]
    first_callback.assert_called_once_with(FILLS_MSG)
    second_callback.assert_called_once_with(FILLS_MSG)


def test_on_open_replays_active_and_queued_subscriptions_once_per_identifier():
    manager, ws = get_connected_manager()
    manager.subscribe(FILLS_SUBSCRIPTION, MagicMock())
    manager.subscribe(FILLS_SUBSCRIPTION, MagicMock())
    manager.on_close(ws, 1006, None)
    manager.subscribe(TWAP_SLICE_FILLS_SUBSCRIPTION, MagicMock())
    ws.send.reset_mock()

    manager.on_open(ws)

    assert get_sent_subscriptions(ws) == [FILLS_SUBSCRIPTION, TWAP_SLICE_FILLS_SUBSCRIPTION]
    assert len(manager.active_subscriptions[subscription_to_identifier(FILLS_SUBSCRIPTION)]) == 2
    assert not manager.queued_subscriptions
    assert manager.ws_ready


def test_error_message_keeps_websocket_running():
    manager, ws = get_connected_manager()

    manager.on_message(ws, json.dumps({"channel": "error", "data": "Already subscribed"}))

    assert not manager.stop_event.is_set()
