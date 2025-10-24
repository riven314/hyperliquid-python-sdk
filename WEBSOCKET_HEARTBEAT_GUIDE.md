# Hyperliquid SDK Websocket Connection Guide

## How the SDK Maintains Websocket Connections

The Hyperliquid Python SDK has a **built-in ping/pong heartbeat mechanism** that automatically keeps websocket connections alive. You don't need to implement this yourself!

### Built-in Heartbeat Mechanism

When you create an `Info` object with `skip_ws=False`, the SDK automatically:

1. **Creates a WebsocketManager** (`hyperliquid/websocket_manager.py:32-33`)
2. **Starts a dedicated ping thread** (`websocket_manager.py:86, 90`)
3. **Sends ping every 50 seconds** (`websocket_manager.py:94`)
4. **Receives and handles pong responses** (`websocket_manager.py:114-116`)

### Code Flow

```python
# When you create Info object
info = Info(api_url, skip_ws=False)
```

This triggers:

```python
# In Info.__init__ (info.py:31-33)
self.ws_manager = WebsocketManager(self.base_url)
self.ws_manager.start()  # Starts websocket thread
```

Which starts the ping thread:

```python
# In WebsocketManager.run() (websocket_manager.py:89-91)
def run(self):
    self.ping_sender.start()  # Starts ping thread
    self.ws.run_forever()     # Runs websocket in this thread

# In WebsocketManager.send_ping() (websocket_manager.py:93-99)
def send_ping(self):
    while not self.stop_event.wait(50):  # Wait 50 seconds between pings
        if not self.ws.keep_running:
            break
        logging.debug("Websocket sending ping")
        self.ws.send(json.dumps({"method": "ping"}))
```

When the server responds:

```python
# In WebsocketManager.on_message() (websocket_manager.py:114-116)
if identifier == "pong":
    logging.debug("Websocket received pong")
    return
```

## Key Improvements in the Updated Script

### 1. Removed Async/Await (Unnecessary)

**Before:**
```python
async def start(self):
    # ...
    while True:
        await asyncio.sleep(1)

asyncio.run(main())
```

**After:**
```python
def start(self):
    # ...
    while self.running.is_set():
        time.sleep(1)

main()  # Simple synchronous call
```

**Why?** The SDK uses threading (not asyncio) for websockets. Mixing asyncio and threading adds unnecessary complexity.

### 2. Added Proper Cleanup

**Before:**
```python
def stop(self):
    print("Monitor stopped")
    # No actual cleanup!
```

**After:**
```python
def stop(self):
    # Unsubscribe from all channels
    for wallet_address, subscription_id in self.subscription_ids.items():
        subscription = {"type": "userFills", "user": wallet_address}
        self.info.unsubscribe(subscription, subscription_id)

    # Disconnect websocket (stops ping thread and closes connection)
    self.info.disconnect_websocket()
```

**Why?** Properly closes the websocket connection and stops the ping thread, preventing resource leaks.

### 3. Added Better Error Handling

**Before:**
```python
except Exception as e:
    print(f"Error: {e}")
```

**After:**
```python
except Exception as e:
    logger.error(f"Error handling user fills: {e}", exc_info=True)
```

**Why?** Uses proper logging with stack traces for better debugging.

### 4. Added Signal Handlers for Graceful Shutdown

**New:**
```python
def signal_handler(signum, frame):
    logger.info(f"\nReceived signal {signum}, initiating shutdown...")
    monitor.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
```

**Why?** Ensures proper cleanup even when killed with SIGTERM (e.g., by systemd or Docker).

### 5. Added Connection Status Logging

**New:**
```python
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG to see ping/pong
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

**Why?** You can set `level=logging.DEBUG` to see the ping/pong messages and verify the heartbeat is working.

## How to See the Heartbeat in Action

To see the ping/pong messages, change the logging level to DEBUG:

```python
logging.basicConfig(level=logging.DEBUG)
```

You'll see output like:
```
2025-10-24 10:00:00 - DEBUG - Websocket sending ping
2025-10-24 10:00:00 - DEBUG - Websocket received pong
2025-10-24 10:00:50 - DEBUG - Websocket sending ping
2025-10-24 10:00:50 - DEBUG - Websocket received pong
```

## Connection Lifecycle

1. **Initialization**: `Info(api_url, skip_ws=False)` creates WebsocketManager and starts it
2. **Connection**: WebsocketManager connects to `wss://api.hyperliquid.xyz/ws`
3. **Heartbeat Starts**: Ping thread begins sending pings every 50 seconds
4. **Subscription**: Your subscriptions are registered and messages routed to callbacks
5. **Active Monitoring**: Connection stays alive via ping/pong
6. **Shutdown**: Call `info.disconnect_websocket()` to cleanly close

## Best Practices

1. **Always call `disconnect_websocket()` on shutdown** - Stops threads and closes connection
2. **Use proper logging** - Set to DEBUG during development to monitor connection
3. **Handle signals** - Implement SIGINT/SIGTERM handlers for graceful shutdown
4. **Track subscription IDs** - Store them so you can unsubscribe if needed
5. **Don't mix asyncio and threading** - The SDK uses threading, stick with it

## Testing the Connection

To test if the connection is being maintained:

```python
# Set debug logging to see pings
logging.basicConfig(level=logging.DEBUG)

# Run your monitor
monitor.start()

# Watch the logs - you should see pings every 50 seconds
# If pings stop, the connection is dead
```

## What If the Connection Drops?

The current SDK implementation:
- ✅ Sends pings every 50 seconds
- ✅ Handles pong responses
- ❌ Does NOT automatically reconnect if connection drops

If you need auto-reconnect, you would need to:
1. Monitor for connection drops (websocket errors)
2. Recreate the Info object
3. Re-subscribe to all channels

This could be a future enhancement to the SDK or your wrapper code.

## Summary

**You don't need to implement heartbeat yourself!** The SDK does it automatically:

- **Ping interval**: 50 seconds
- **Mechanism**: Dedicated thread sends `{"method": "ping"}`
- **Response**: Server sends back pong, SDK logs it
- **Threading**: Uses threads, not asyncio
- **Cleanup**: Call `info.disconnect_websocket()` when done

The improved script leverages all these SDK features while adding proper error handling, cleanup, and logging.
