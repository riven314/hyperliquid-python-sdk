import pytest

from hyperliquid.websocket_manager import subscription_to_identifier, ws_msg_to_identifier

MIXED_CASE_USER = "0xAF0fdd39e5d92499b0ed9f68693da99c0ec1e92e"
FILL = {
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


@pytest.mark.parametrize(
    "subscription, ws_msg, expected_identifier",
    [
        (
            {"type": "userFills", "user": MIXED_CASE_USER},
            {"channel": "userFills", "data": {"user": MIXED_CASE_USER, "isSnapshot": False, "fills": [FILL]}},
            f"userFills:{MIXED_CASE_USER.lower()}",
        ),
        (
            {"type": "userTwapSliceFills", "user": MIXED_CASE_USER},
            {
                "channel": "userTwapSliceFills",
                "data": {
                    "user": MIXED_CASE_USER,
                    "isSnapshot": False,
                    "twapSliceFills": [{"twapId": 2194823, "fill": FILL}],
                },
            },
            f"userTwapSliceFills:{MIXED_CASE_USER.lower()}",
        ),
    ],
)
def test_user_keyed_subscription_and_message_share_identifier(subscription, ws_msg, expected_identifier):
    assert subscription_to_identifier(subscription) == expected_identifier
    assert ws_msg_to_identifier(ws_msg) == expected_identifier
