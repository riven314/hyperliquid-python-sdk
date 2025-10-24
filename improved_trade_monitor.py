#!/usr/bin/env python3
"""
Hyperliquid Copy Trading Monitor - Improved Version
Monitors live trade events from multiple wallets via WebSocket for copy trading

Key improvements:
- Proper websocket connection management with SDK's built-in heartbeat
- Better error handling and logging
- Graceful shutdown with cleanup
- Threading-based approach (aligned with SDK's websocket implementation)
"""

import json
import logging
import signal
import sqlite3
import sys
import threading
import time
from typing import List, Dict, Any, Callable
from datetime import datetime
from hyperliquid.info import Info
from hyperliquid.utils import constants

# Configure logging to see websocket heartbeat activity
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG to see ping/pong messages
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def init_database():
    """Initialize SQLite database and create trades table"""
    conn = sqlite3.connect("hyperliquid_trades.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            wallet_address TEXT,
            coin TEXT,
            side TEXT,
            dir TEXT,
            price REAL,
            size REAL,
            crossed INTEGER,
            closed_pnl REAL,
            fee REAL,
            fee_token TEXT,
            order_id TEXT,
            trade_id TEXT,
            hash TEXT,
            is_snapshot INTEGER,
            received_at TEXT,
            raw_data TEXT
        )
    """
    )

    conn.commit()
    conn.close()
    logger.info("✓ Database initialized: hyperliquid_trades.db")


class HyperliquidTradeMonitor:
    """Monitor live trades from multiple Hyperliquid wallets with proper connection management"""

    def __init__(self, wallet_addresses: List[str], testnet: bool = False):
        """
        Initialize the trade monitor

        Args:
            wallet_addresses: List of wallet addresses to monitor
            testnet: Whether to use testnet (default: False for mainnet)
        """
        self.wallet_addresses = [addr.lower() for addr in wallet_addresses]
        self.api_url = (
            constants.TESTNET_API_URL if testnet else constants.MAINNET_API_URL
        )

        # Initialize Info client with WebSocket support
        # The SDK automatically starts a websocket connection with ping/pong heartbeat (every 50s)
        self.info = Info(self.api_url, skip_ws=False)

        # Store callbacks for different event types
        self.fill_callbacks: List[Callable] = []
        self.error_callbacks: List[Callable] = []

        # Track subscription IDs for potential cleanup
        self.subscription_ids: Dict[str, int] = {}

        # Flag to track if we're running
        self.running = threading.Event()

        logger.info(f"Initialized monitor for {len(self.wallet_addresses)} wallets")
        logger.info(f"Using {'TESTNET' if testnet else 'MAINNET'}")
        logger.info("Websocket heartbeat: SDK automatically sends ping every 50 seconds")

    def on_fill(self, callback: Callable[[str, Dict[str, Any]], None]):
        """
        Register a callback for fill events

        Args:
            callback: Function that takes (wallet_address, fill_data) as arguments
        """
        self.fill_callbacks.append(callback)

    def on_error(self, callback: Callable[[str, Exception], None]):
        """
        Register a callback for error events

        Args:
            callback: Function that takes (wallet_address, error) as arguments
        """
        self.error_callbacks.append(callback)

    def _handle_user_fills(self, wallet_address: str, message: Any):
        """
        Handle incoming user fill messages

        Args:
            wallet_address: The wallet address this message is for
            message: The WebSocket message containing fill data
        """
        try:
            # Check if this is a snapshot (historical data) or live data
            is_snapshot = message.get("isSnapshot", False)

            if "fills" in message:
                fills = message["fills"]

                for fill in fills:
                    # Add metadata
                    fill["wallet_address"] = wallet_address
                    fill["is_snapshot"] = is_snapshot
                    fill["received_at"] = datetime.now().isoformat()

                    # Call all registered callbacks
                    for callback in self.fill_callbacks:
                        try:
                            callback(wallet_address, fill)
                        except Exception as e:
                            logger.error(f"Error in fill callback: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Error handling user fills for {wallet_address}: {e}", exc_info=True)
            for callback in self.error_callbacks:
                try:
                    callback(wallet_address, e)
                except Exception as callback_error:
                    logger.error(f"Error in error callback: {callback_error}")

    def start(self):
        """Start monitoring all configured wallets"""
        logger.info("\n=== Starting Hyperliquid Trade Monitor ===")
        logger.info(f"Monitoring {len(self.wallet_addresses)} wallet(s):\n")

        for addr in self.wallet_addresses:
            logger.info(f"  - {addr}")

        logger.info("\nSubscribing to live trade events via WebSocket...")

        # Subscribe to user fills for each wallet
        for wallet_address in self.wallet_addresses:
            subscription = {"type": "userFills", "user": wallet_address}

            def make_handler(addr):
                def handler(msg):
                    # Ignore the subscribe ack
                    if msg.get("channel") == "subscriptionResponse":
                        logger.debug(f"Subscription confirmed for {addr}")
                        return

                    # Unwrap data for all normal messages
                    data = msg.get("data", msg)

                    # Only forward userFills payloads
                    if isinstance(data, dict) and (
                        "fills" in data or data.get("isSnapshot") is not None
                    ):
                        self._handle_user_fills(addr, data)
                    else:
                        # Log unexpected message formats at debug level
                        logger.debug(f"Unhandled WS message for {addr}: {msg}")

                return handler

            try:
                # Subscribe to this wallet's fills
                subscription_id = self.info.subscribe(subscription, make_handler(wallet_address))
                self.subscription_ids[wallet_address] = subscription_id
                logger.info(f"✓ Subscribed to fills for {wallet_address} (ID: {subscription_id})")
            except Exception as e:
                logger.error(f"Failed to subscribe for {wallet_address}: {e}", exc_info=True)

        logger.info("\n=== Monitoring Active - Press Ctrl+C to stop ===")
        logger.info("Connection is kept alive by SDK's automatic ping/pong (every 50s)\n")

        # Set running flag
        self.running.set()

        # Keep the main thread alive
        try:
            while self.running.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n\nReceived interrupt signal, stopping monitor...")
            self.stop()

    def stop(self):
        """Stop monitoring and clean up websocket connection"""
        logger.info("Stopping monitor and cleaning up...")

        # Clear running flag
        self.running.clear()

        # Unsubscribe from all channels (optional, but good practice)
        for wallet_address, subscription_id in self.subscription_ids.items():
            try:
                subscription = {"type": "userFills", "user": wallet_address}
                success = self.info.unsubscribe(subscription, subscription_id)
                if success:
                    logger.info(f"✓ Unsubscribed from {wallet_address}")
                else:
                    logger.warning(f"Failed to unsubscribe from {wallet_address}")
            except Exception as e:
                logger.error(f"Error unsubscribing from {wallet_address}: {e}")

        # Disconnect the websocket (stops ping thread and closes connection)
        try:
            self.info.disconnect_websocket()
            logger.info("✓ Websocket disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting websocket: {e}")

        logger.info("Monitor stopped cleanly")


# Example usage and callbacks
def print_fill_details(wallet_address: str, fill: Dict[str, Any]):
    """Example callback that prints fill details"""

    # Skip snapshot data if you only want live trades
    if fill.get("is_snapshot", False):
        return

    print("\n" + "=" * 80)
    print(f"🔔 NEW TRADE DETECTED")
    print("=" * 80)
    print(f"Wallet:       {wallet_address}")
    print(
        f"Time:         {datetime.fromtimestamp(fill['time']/1000).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print(f"Coin:         {fill['coin']}")
    print(f"Side:         {fill['side']} ({fill['dir']})")
    print(f"Price:        ${fill['px']}")
    print(f"Size:         {fill['sz']}")
    print(f"Direction:    {fill['dir']}")
    print(f"Crossed:      {'Yes' if fill.get('crossed', False) else 'No'}")

    if "closedPnl" in fill and fill["closedPnl"] != "0.0":
        print(f"Closed PnL:   ${fill['closedPnl']}")

    print(f"Fee:          ${fill.get('fee', '0')} {fill.get('feeToken', 'USDC')}")
    print(f"Order ID:     {fill['oid']}")
    print(f"Trade ID:     {fill['tid']}")
    print(f"Hash:         {fill['hash']}")
    print("=" * 80 + "\n")


def save_to_database(wallet_address: str, fill: Dict[str, Any]):
    """Callback that saves trade data to SQLite database"""
    if fill.get("is_snapshot", False):
        return

    try:
        conn = sqlite3.connect("hyperliquid_trades.db")
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO trades (
                timestamp, wallet_address, coin, side, dir, price, size,
                crossed, closed_pnl, fee, fee_token, order_id, trade_id,
                hash, is_snapshot, received_at, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                datetime.fromtimestamp(fill["time"] / 1000).isoformat(),
                wallet_address,
                fill.get("coin", ""),
                fill.get("side", ""),
                fill.get("dir", ""),
                float(fill.get("px", 0)),
                float(fill.get("sz", 0)),
                1 if fill.get("crossed", False) else 0,
                float(fill.get("closedPnl", 0)),
                float(fill.get("fee", 0)),
                fill.get("feeToken", ""),
                fill.get("oid", ""),
                fill.get("tid", ""),
                fill.get("hash", ""),
                1 if fill.get("is_snapshot", False) else 0,
                fill.get("received_at", ""),
                json.dumps(fill),
            ),
        )

        conn.commit()
        conn.close()

    except Exception as e:
        logger.error(f"Error saving to database: {e}", exc_info=True)


def copy_trade_logic(wallet_address: str, fill: Dict[str, Any]):
    """
    Example callback for copy trading logic

    IMPORTANT: This is just a template. You'll need to:
    1. Add your own trading logic
    2. Handle position sizing
    3. Implement risk management
    4. Add error handling
    5. Consider slippage and timing
    """

    # Skip snapshot data
    if fill.get("is_snapshot", False):
        return

    logger.info(f"\n⚠️  COPY TRADE SIGNAL from {wallet_address}")
    logger.info(f"   Would execute: {fill['dir']} {fill['sz']} {fill['coin']} @ ${fill['px']}")
    logger.info(f"   Side: {fill['side']}")

    # TODO: Implement your actual trading logic here
    # Example pseudo-code:
    #
    # if should_copy_this_trade(wallet_address, fill):
    #     scaled_size = calculate_position_size(fill['sz'])
    #     exchange.place_order(
    #         coin=fill['coin'],
    #         side=fill['side'],
    #         size=scaled_size,
    #         order_type='Market'  # or limit based on your strategy
    #     )


def main():
    """Main function to run the monitor"""

    # Initialize database
    init_database()

    # List of wallet addresses to monitor
    WALLETS_TO_MONITOR = [
        # GOOD WALLETS
        "0x782e432267376f377585fc78092d998f8442ab83",
        "0x5846ac5619d2a762751b1663d86a402085505765",
        "0xcc64e48df8560565f0f87bec36b87b6a6f171ebf",
        # "0x6beffb9bec3364ae579fa7cb864effefa7bf2695",
        "0x6049ddfd35e4d9098a6367e36023f2a36792b642",
        "0xeda73c9fa6a90cea2bcc5529e9e12c012e13655b",
        "0x3c16fc881bbbde5771e9026fc888bf5493e3b4b1",
        "0x11d67fa925877813b744abc0917900c2b1d6eb81",
        "0x406073860818ecfb7dca0706a6bac73faa95bd3e",
        "0x064e89061ce80cb1b41a2d2963d48d2e8cabde4a",
        "0x1a5612386dce50174bd467498afa607df418d7c6",
        # MY WALLET FOR TESTING
        # "0xe871bc4D06E9337fD5611c28812e7E29478E9145",
    ]

    # Initialize the monitor
    # Set testnet=True if you want to test on testnet first
    monitor = HyperliquidTradeMonitor(
        wallet_addresses=WALLETS_TO_MONITOR,
        testnet=False  # Change to True for testnet
    )

    # Register callbacks
    monitor.on_fill(print_fill_details)  # Print trade details to console
    monitor.on_fill(save_to_database)  # Save trades to database
    # monitor.on_fill(copy_trade_logic)  # Uncomment to enable copy trading

    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"\nReceived signal {signum}, initiating shutdown...")
        monitor.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start monitoring (this blocks until interrupted)
    try:
        monitor.start()
    except Exception as e:
        logger.error(f"Fatal error in monitor: {e}", exc_info=True)
        monitor.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
