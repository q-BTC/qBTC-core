"""
Redis Event Bridge — cross-process communication between Node and Web processes.

Node side (NodeEventBridge):
  - publish_event()           → publishes to qbtc:events channel
  - notify_db_updated()       → publishes to qbtc:db_updated channel
  - listen_tx_submissions()   → BRPOP on qbtc:tx_submit, pushes results to qbtc:tx_result:{id}

Web side (WebEventBridge):
  - subscribe_events()        → subscribes to qbtc:events, emits on local event_bus
  - subscribe_db_updates()    → subscribes to qbtc:db_updated, calls try_catch_up_with_primary()
  - submit_transaction(tx)    → LPUSH to qbtc:tx_submit, BRPOP on qbtc:tx_result:{id}
"""

import json
import uuid
import logging
import asyncio
import time
from typing import Any, Dict, Optional

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Channel / key names
EVENTS_CHANNEL = "qbtc:events"
DB_UPDATED_CHANNEL = "qbtc:db_updated"
TX_SUBMIT_QUEUE = "qbtc:tx_submit"
TX_RESULT_PREFIX = "qbtc:tx_result:"
GOSSIP_STATE_KEY = "qbtc:gossip_state"

# Global bridge references (set by the process that creates them)
node_bridge: Optional["NodeEventBridge"] = None
web_bridge: Optional["WebEventBridge"] = None


class NodeEventBridge:
    """Node-side Redis bridge: publishes events and processes transaction submissions."""

    def __init__(self, redis_url: str):
        if not REDIS_AVAILABLE or not redis_url:
            raise RuntimeError("redis.asyncio is required for NodeEventBridge")
        self.redis_url = redis_url
        self._pub_client: Optional[aioredis.Redis] = None
        self._sub_client: Optional[aioredis.Redis] = None

    async def _get_pub_client(self) -> aioredis.Redis:
        if self._pub_client is None:
            self._pub_client = aioredis.from_url(
                self.redis_url, decode_responses=True
            )
        return self._pub_client

    async def publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish an event to Redis for the web process."""
        try:
            client = await self._get_pub_client()
            payload = json.dumps({"type": event_type, "data": data})
            await client.publish(EVENTS_CHANNEL, payload)
            logger.debug(f"Published event to Redis: {event_type}")
        except Exception as e:
            logger.warning(f"Failed to publish event to Redis: {e}")

    async def notify_db_updated(self):
        """Notify web process that the DB has been updated (new block, etc.)."""
        try:
            client = await self._get_pub_client()
            await client.publish(DB_UPDATED_CHANNEL, "updated")
            logger.debug("Published db_updated notification")
        except Exception as e:
            logger.warning(f"Failed to publish db_updated: {e}")

    async def publish_gossip_state(self, state: Dict[str, Any]):
        """Write gossip state to a Redis key for the web process to read."""
        try:
            client = await self._get_pub_client()
            await client.set(GOSSIP_STATE_KEY, json.dumps(state), ex=30)
            logger.debug("Published gossip state to Redis")
        except Exception as e:
            logger.warning(f"Failed to publish gossip state: {e}")

    async def listen_tx_submissions(self):
        """
        Long-running loop: BRPOP on qbtc:tx_submit, process each transaction
        via the node's mempool, push results to qbtc:tx_result:{request_id}.
        """
        # Store client as instance var for proper lifecycle management
        self._tx_client = aioredis.from_url(self.redis_url, decode_responses=True)
        client = self._tx_client

        # Verify Redis connectivity before entering the loop
        try:
            await client.ping()
            logger.info("NodeEventBridge: Redis connection verified, listening for tx submissions")
        except Exception as e:
            logger.error(f"NodeEventBridge: Redis connection FAILED: {e}")
            return

        last_heartbeat = time.time()

        while True:
            try:
                # Periodic heartbeat log so we can confirm the loop is alive
                now = time.time()
                if now - last_heartbeat > 30:
                    last_heartbeat = now
                    qlen = await client.llen(TX_SUBMIT_QUEUE)
                    logger.info(f"[BRIDGE] TX listener alive, queue length: {qlen}")

                result = await client.brpop(TX_SUBMIT_QUEUE, timeout=1)
                if result is None:
                    continue

                _key, raw = result
                logger.info(f"[BRIDGE] Received tx submission from Redis queue")
                submission = json.loads(raw)
                request_id = submission.get("request_id")
                transaction = submission.get("transaction")

                if not request_id or not transaction:
                    logger.warning("[BRIDGE] Malformed tx submission, skipping")
                    continue

                txid = transaction.get("txid", "unknown")
                logger.info(f"[BRIDGE] Processing tx {txid} (request_id={request_id[:8]}...)")

                # Process with a 4s timeout to prevent broadcast from blocking the listener
                try:
                    reply = await asyncio.wait_for(
                        self._process_tx_submission(transaction),
                        timeout=4.0
                    )
                except asyncio.TimeoutError:
                    logger.error(f"[BRIDGE] Processing tx {txid} timed out after 4s")
                    reply = {"status": "error", "error": "Transaction processing timed out on node"}

                reply["request_id"] = request_id

                result_key = f"{TX_RESULT_PREFIX}{request_id}"
                await client.lpush(result_key, json.dumps(reply))
                await client.expire(result_key, 30)
                logger.info(f"[BRIDGE] Sent reply for tx {txid}: {reply.get('status')}")

            except asyncio.CancelledError:
                logger.info("TX submission listener cancelled")
                break
            except Exception as e:
                logger.error(f"Error processing tx submission: {e}", exc_info=True)
                # Reconnect Redis client if connection broke
                try:
                    await client.ping()
                except Exception:
                    logger.warning("[BRIDGE] Redis connection lost, reconnecting...")
                    try:
                        await client.aclose()
                    except Exception:
                        pass
                    client = aioredis.from_url(self.redis_url, decode_responses=True)
                    self._tx_client = client
                await asyncio.sleep(0.5)

        await client.aclose()

    async def _process_tx_submission(self, transaction: Dict) -> Dict:
        """Add transaction to mempool and broadcast via gossip."""
        try:
            from state.state import mempool_manager
            from events.event_bus import event_bus, EventTypes

            txid = transaction.get("txid")
            logger.info(f"[BRIDGE] _process_tx_submission called for {txid}")

            success, error = mempool_manager.add_transaction(transaction)
            if not success:
                logger.warning(f"[BRIDGE] Rejected transaction {txid}: {error}")
                return {"status": "error", "error": f"Transaction rejected: {error}"}

            logger.info(f"[BRIDGE] Added transaction {txid} to mempool. Size: {mempool_manager.size()}")

            # Emit mempool event locally (for RPC/node-side consumers)
            event_data = {
                "txid": txid,
                "transaction": transaction,
                "sender": transaction.get("sender"),
                "receiver": transaction.get("receiver"),
                "amount": transaction.get("amount"),
            }
            await event_bus.emit(EventTypes.TRANSACTION_PENDING, event_data, source="bridge")

            # Publish to Redis so the web process picks it up
            # (populates _mempool_cache and triggers WebSocket broadcasts)
            await self.publish_event(EventTypes.TRANSACTION_PENDING, event_data)

            # Broadcast to network (fire-and-forget with timeout — don't block the reply)
            import sys
            gossip_node = getattr(sys.modules.get("__main__", None), "gossip_node", None)
            if gossip_node:
                try:
                    await asyncio.wait_for(
                        gossip_node.randomized_broadcast(transaction),
                        timeout=2.0
                    )
                    logger.info(f"[BRIDGE] Transaction {txid} broadcast to network")
                except asyncio.TimeoutError:
                    logger.warning(f"[BRIDGE] Gossip broadcast timed out for {txid} (tx still in mempool)")
            else:
                logger.warning(f"[BRIDGE] No gossip node — tx {txid} in mempool only")

            return {"status": "success", "txid": txid}

        except Exception as e:
            logger.error(f"Error processing transaction: {e}", exc_info=True)
            return {"status": "error", "error": str(e)}

    async def close(self):
        for client in (self._pub_client, self._sub_client, getattr(self, '_tx_client', None)):
            if client:
                try:
                    await client.aclose()
                except Exception:
                    pass


class WebEventBridge:
    """Web-side Redis bridge: subscribes to events and submits transactions."""

    def __init__(self, redis_url: str):
        if not REDIS_AVAILABLE or not redis_url:
            raise RuntimeError("redis.asyncio is required for WebEventBridge")
        self.redis_url = redis_url
        self._client: Optional[aioredis.Redis] = None

    async def _get_client(self) -> aioredis.Redis:
        if self._client is None:
            self._client = aioredis.from_url(
                self.redis_url, decode_responses=True
            )
        return self._client

    async def subscribe_events(self):
        """Subscribe to qbtc:events and re-emit on local event_bus."""
        from events.event_bus import event_bus

        client = aioredis.from_url(self.redis_url, decode_responses=True)
        pubsub = client.pubsub()
        await pubsub.subscribe(EVENTS_CHANNEL)
        logger.info("WebEventBridge: subscribed to qbtc:events")

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    payload = json.loads(message["data"])
                    event_type = payload.get("type")
                    data = payload.get("data", {})
                    await event_bus.emit(event_type, data, source="redis_bridge")
                    logger.debug(f"Re-emitted event from Redis: {event_type}")
                except json.JSONDecodeError:
                    logger.warning("Non-JSON message on qbtc:events")
                except Exception as e:
                    logger.error(f"Error re-emitting event: {e}")
        except asyncio.CancelledError:
            logger.info("Event subscription cancelled")
        finally:
            await pubsub.unsubscribe(EVENTS_CHANNEL)
            await pubsub.aclose()
            await client.aclose()

    async def subscribe_db_updates(self):
        """Subscribe to qbtc:db_updated and call try_catch_up_with_primary()."""
        from database.database import try_catch_up

        client = aioredis.from_url(self.redis_url, decode_responses=True)
        pubsub = client.pubsub()
        await pubsub.subscribe(DB_UPDATED_CHANNEL)
        logger.info("WebEventBridge: subscribed to qbtc:db_updated")

        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                try:
                    try_catch_up()
                    logger.debug("Caught up with primary after db_updated signal")
                except Exception as e:
                    logger.warning(f"Catch-up failed after db_updated: {e}")
        except asyncio.CancelledError:
            logger.info("DB update subscription cancelled")
        finally:
            await pubsub.unsubscribe(DB_UPDATED_CHANNEL)
            await pubsub.aclose()
            await client.aclose()

    async def submit_transaction(self, transaction: Dict) -> Dict:
        """
        Submit a transaction to the node process via Redis queue.
        Returns {"status": "success", "txid": ...} or {"status": "error", "error": ...}.
        """
        client = await self._get_client()
        request_id = str(uuid.uuid4())
        txid = transaction.get("txid", "unknown")

        submission = json.dumps({
            "request_id": request_id,
            "transaction": transaction,
        })

        result_key = f"{TX_RESULT_PREFIX}{request_id}"

        try:
            await client.lpush(TX_SUBMIT_QUEUE, submission)
            logger.info(f"[WEB_BRIDGE] Submitted tx {txid} to Redis queue (request_id={request_id[:8]}...)")

            # Wait for result with 8s timeout (node has 4s processing budget + margin)
            result = await client.brpop(result_key, timeout=8)
            if result is None:
                # Check queue length to help diagnose
                qlen = await client.llen(TX_SUBMIT_QUEUE)
                logger.error(f"[WEB_BRIDGE] Timeout waiting for tx {txid} reply (queue length: {qlen})")
                return {"status": "error", "error": "Transaction submission timed out (node did not respond in 8s)"}

            _key, raw = result
            reply = json.loads(raw)
            logger.info(f"[WEB_BRIDGE] Got reply for tx {txid}: {reply.get('status')}")
            return reply

        except Exception as e:
            logger.error(f"Error submitting transaction via Redis: {e}")
            return {"status": "error", "error": str(e)}

    async def get_gossip_state(self) -> Optional[Dict]:
        """Read gossip state from Redis key."""
        try:
            client = await self._get_client()
            raw = await client.get(GOSSIP_STATE_KEY)
            if raw:
                return json.loads(raw)
            return None
        except Exception as e:
            logger.warning(f"Failed to read gossip state: {e}")
            return None

    async def get_mempool_stats(self) -> Optional[Dict]:
        """Read mempool stats published by node to Redis."""
        try:
            client = await self._get_client()
            raw = await client.get("qbtc:mempool_stats")
            if raw:
                return json.loads(raw)
            return None
        except Exception as e:
            logger.warning(f"Failed to read mempool stats: {e}")
            return None

    async def close(self):
        if self._client:
            await self._client.aclose()
