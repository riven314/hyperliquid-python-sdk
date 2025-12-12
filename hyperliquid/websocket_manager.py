import json
import logging
import random
import threading
import time
from collections import defaultdict

import websocket

from hyperliquid.utils.types import Any, Callable, Dict, List, NamedTuple, Optional, Subscription, Tuple, WsMsg

ActiveSubscription = NamedTuple(
    "ActiveSubscription",
    [
        ("callback", Callable[[Any], None]),
        ("subscription_id", int),
        ("subscription", Subscription),  # store original subscription for reconnection
    ],
)


def subscription_to_identifier(subscription: Subscription) -> str:
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"].lower()}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"].lower()}'
    elif subscription["type"] == "userEvents":
        return "userEvents"
    elif subscription["type"] == "userFills":
        return f'userFills:{subscription["user"].lower()}'
    elif subscription["type"] == "candle":
        return f'candle:{subscription["coin"].lower()},{subscription["interval"]}'
    elif subscription["type"] == "orderUpdates":
        return "orderUpdates"
    elif subscription["type"] == "userFundings":
        return f'userFundings:{subscription["user"].lower()}'
    elif subscription["type"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{subscription["user"].lower()}'
    elif subscription["type"] == "webData2":
        return f'webData2:{subscription["user"].lower()}'
    elif subscription["type"] == "bbo":
        return f'bbo:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetCtx":
        return f'activeAssetCtx:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetData":
        return f'activeAssetData:{subscription["coin"].lower()},{subscription["user"].lower()}'


def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
    if ws_msg["channel"] == "subscriptionResponse":
        return "subscriptionResponse"
    elif ws_msg["channel"] == "error":
        return "error"
    elif ws_msg["channel"] == "pong":
        return "pong"
    elif ws_msg["channel"] == "allMids":
        return "allMids"
    elif ws_msg["channel"] == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"].lower()}'
    elif ws_msg["channel"] == "user":
        return "userEvents"
    elif ws_msg["channel"] == "userFills":
        return f'userFills:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "candle":
        return f'candle:{ws_msg["data"]["s"].lower()},{ws_msg["data"]["i"]}'
    elif ws_msg["channel"] == "orderUpdates":
        return "orderUpdates"
    elif ws_msg["channel"] == "userFundings":
        return f'userFundings:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "webData2":
        return f'webData2:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "bbo":
        return f'bbo:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetCtx" or ws_msg["channel"] == "activeSpotAssetCtx":
        return f'activeAssetCtx:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetData":
        return f'activeAssetData:{ws_msg["data"]["coin"].lower()},{ws_msg["data"]["user"].lower()}'


class WebsocketManager(threading.Thread):
    def __init__(
        self,
        base_url: str,
        reconnect_enabled: bool = True,
        max_reconnect_delay: int = 30,
        pong_timeout: float = 90.0,
    ):
        super().__init__()
        # stable identifier for log correlation across long-running processes
        self.instance_id = f"{random.getrandbits(24):06x}"
        self.connection_epoch = 0
        self.subscription_id_counter = 0
        self.ws_ready = False
        self.queued_subscriptions: List[Tuple[Subscription, ActiveSubscription]] = []
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)

        # reconnection configuration
        self.reconnect_enabled = reconnect_enabled
        self.max_reconnect_delay = max_reconnect_delay
        self.reconnect_attempts = 0

        # pong timeout configuration
        self.pong_timeout = pong_timeout
        self.last_pong_time = time.time()
        self.heartbeat_interval = 30  # seconds

        ws_url = "ws" + base_url[len("http") :] + "/ws"
        self.ws = websocket.WebSocketApp(
            ws_url, on_message=self.on_message, on_open=self.on_open, on_error=self.on_error, on_close=self.on_close
        )
        self.ping_sender: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

    @property
    def log_prefix(self) -> str:
        return f"[ws:{self.instance_id} e:{self.connection_epoch}]"

    def _send_heartbeat_once(self, context: str) -> None:
        """Send one heartbeat immediately (best-effort)."""
        if not self.ws.keep_running:
            logging.debug(f"{self.log_prefix} HEARTBEAT_SKIPPED self.ws.keep_running=False context={context}")
            return
        try:
            self.ws.send(json.dumps({"method": "ping"}))
            logging.debug(f"{self.log_prefix} HEARTBEAT_SENT context={context}")
        except Exception as e:
            logging.error(f"{self.log_prefix} HEARTBEAT_SEND_FAILED context={context} error={e}")

    def on_error(self, ws, error):
        err_str = str(error)
        if "Inactive" in err_str:
            logging.error(
                f"{self.log_prefix} WS_INACTIVE_SIGNAL error={err_str} last_pong_age_s={time.time() - self.last_pong_time:.1f}"
            )
        logging.error(f"{self.log_prefix} WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        close_msg_str = "" if close_msg is None else str(close_msg)
        if "Inactive" in close_msg_str:
            logging.error(
                f"{self.log_prefix} WS_CLOSED_INACTIVE code={close_status_code} msg={close_msg_str} last_pong_age_s={time.time() - self.last_pong_time:.1f}"
            )
        logging.info(f"{self.log_prefix} WebSocket closed: {close_status_code} - {close_msg}")
        self.ws_ready = False

    def run(self):
        logging.info(
            f"{self.log_prefix} running WebsocketManager reconnect_enabled={self.reconnect_enabled} max_reconnect_delay={self.max_reconnect_delay} "
            f"pong_timeout={self.pong_timeout} heartbeat_interval={self.heartbeat_interval}"
        )
        self._start_ping_thread()

        while not self.stop_event.is_set():
            try:
                self.ws.run_forever()

                # connection closed - check if we should reconnect
                if self.stop_event.is_set() or not self.reconnect_enabled:
                    break

                # calculate exponential backoff delay, apply full jitter to de-synchronize retries
                base_delay = min(2**self.reconnect_attempts, self.max_reconnect_delay)
                delay = random.uniform(0.0, base_delay)
                self.reconnect_attempts += 1

                logging.info(
                    f"{self.log_prefix} Reconnecting in {delay:.2f}s (attempt {self.reconnect_attempts}, base_delay={base_delay}s, jitter=full)..."
                )

                # wait with backoff (interruptible by stop_event)
                if self.stop_event.wait(delay):
                    break

                # loop continues, ws.run_forever() will reconnect

            except Exception as e:
                logging.error(f"{self.log_prefix} WebSocket unexpected error: {e}")
                if not self.stop_event.is_set() and self.reconnect_enabled:
                    # calculate exponential backoff delay, apply full jitter to de-synchronize retries
                    base_delay = min(2**self.reconnect_attempts, self.max_reconnect_delay)
                    delay = random.uniform(0.0, base_delay)
                    self.reconnect_attempts += 1

                    logging.info(
                        f"{self.log_prefix} Reconnecting after error in {delay:.2f}s (attempt {self.reconnect_attempts}, base_delay={base_delay}s, jitter=full)..."
                    )

                    if self.stop_event.wait(delay):
                        break
                    continue
                break

    def send_ping(self):
        logging.info(f"{self.log_prefix} Websocket ping loop running interval={self.heartbeat_interval}s")
        while not self.stop_event.wait(self.heartbeat_interval):
            if not self.ws.keep_running:
                continue

            # check pong timeout
            time_since_pong = time.time() - self.last_pong_time
            if time_since_pong > self.pong_timeout:
                logging.error(
                    f"{self.log_prefix} Pong timeout - no response for {time_since_pong:.1f}s; closing socket"
                )
                self.ws.close()  # trigger reconnection via run()
                continue

            # send heartbeat (same payload as immediate heartbeat)
            self._send_heartbeat_once(context="interval")

        logging.info(f"{self.log_prefix} Websocket ping thread stopped")

    def stop(self):
        self.stop_event.set()
        self.ws.close()
        if self.ping_sender is not None and self.ping_sender.is_alive():
            self.ping_sender.join()

    def on_message(self, _ws, message):
        if message == "Websocket connection established.":
            logging.debug(message)
            return

        ws_msg: WsMsg = json.loads(message)

        # check channel directly for proper type narrowing
        if ws_msg["channel"] == "error":
            logging.error(f"Websocket received error message, stopping websocket: {ws_msg['data']}")
            self.stop()
            return

        if ws_msg["channel"] == "subscriptionResponse":
            logging.debug(f"Websocket received subscription response: {ws_msg['data']}")
            return

        # get identifier for remaining message types
        identifier = ws_msg_to_identifier(ws_msg)
        if identifier == "pong":
            self.last_pong_time = time.time()  # update pong timestamp
            logging.debug("Websocket received pong")
            return

        if identifier is None:
            logging.debug(f"Websocket not handling unknown message: {ws_msg}")
            return

        active_subscriptions = self.active_subscriptions[identifier]
        if len(active_subscriptions) == 0:
            print("Websocket message from an unexpected subscription:", message, identifier)
        else:
            for active_subscription in active_subscriptions:
                active_subscription.callback(ws_msg)

    def on_open(self, _ws):
        self.connection_epoch += 1
        logging.info(f"{self.log_prefix} on_open")
        self.ws_ready = True
        self.last_pong_time = time.time()  # reset pong timer on new connection
        if self.ping_sender is None or not self.ping_sender.is_alive():
            self._start_ping_thread()
            logging.info(f"{self.log_prefix} Restarted ping thread after reconnect")

        # send an immediate heartbeat on every successful open/reconnect
        self._send_heartbeat_once(context="on_open")

        # on reconnection, restore active subscriptions
        if self.reconnect_attempts > 0:
            logging.info(f"{self.log_prefix} Reconnected successfully after {self.reconnect_attempts} attempts")

            # collect all active subscriptions to restore
            subscriptions_to_restore = []

            # save current active subscriptions and clear them
            temp_active_subs = dict(self.active_subscriptions)
            self.active_subscriptions.clear()

            # re-subscribe to all previously active subscriptions
            for identifier, active_subs in temp_active_subs.items():
                for active_sub in active_subs:
                    subscriptions_to_restore.append(active_sub)

            # restore all subscriptions
            for active_sub in subscriptions_to_restore:
                self.subscribe(active_sub.subscription, active_sub.callback, active_sub.subscription_id)

            logging.info(f"{self.log_prefix} Restored {len(subscriptions_to_restore)} subscriptions after reconnection")

        # reset reconnection counter on successful connection
        self.reconnect_attempts = 0

        # process queued subscriptions (for initial connection or new subscriptions during disconnect)
        for subscription, active_subscription in self.queued_subscriptions:
            self.subscribe(subscription, active_subscription.callback, active_subscription.subscription_id)

        self.queued_subscriptions.clear()

    def _start_ping_thread(self):
        if self.ping_sender is not None and self.ping_sender.is_alive():
            return
        self.ping_sender = threading.Thread(target=self.send_ping, name="HyperliquidPing", daemon=True)
        self.ping_sender.start()

    def subscribe(
        self, subscription: Subscription, callback: Callable[[Any], None], subscription_id: Optional[int] = None
    ) -> int:
        if subscription_id is None:
            self.subscription_id_counter += 1
            subscription_id = self.subscription_id_counter
        if not self.ws_ready:
            logging.debug("enqueueing subscription")
            self.queued_subscriptions.append(
                (subscription, ActiveSubscription(callback, subscription_id, subscription))
            )
        else:
            logging.debug("subscribing")
            identifier = subscription_to_identifier(subscription)
            if identifier == "userEvents" or identifier == "orderUpdates":
                # TODO: ideally the userEvent and orderUpdates messages would include the user so that we can multiplex
                if len(self.active_subscriptions[identifier]) != 0:
                    raise NotImplementedError(f"Cannot subscribe to {identifier} multiple times")
            self.active_subscriptions[identifier].append(ActiveSubscription(callback, subscription_id, subscription))
            self.ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
        return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        if not self.ws_ready:
            raise NotImplementedError("Can't unsubscribe before websocket connected")
        identifier = subscription_to_identifier(subscription)
        active_subscriptions = self.active_subscriptions[identifier]
        new_active_subscriptions = [x for x in active_subscriptions if x.subscription_id != subscription_id]
        if len(new_active_subscriptions) == 0:
            self.ws.send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
        self.active_subscriptions[identifier] = new_active_subscriptions
        return len(active_subscriptions) != len(new_active_subscriptions)
