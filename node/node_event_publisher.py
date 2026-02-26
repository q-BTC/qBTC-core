"""
Node-side event publisher — periodically publishes gossip network state
and mempool stats to Redis so the web process can serve /debug/network,
/debug/peers, /health, and /debug/mempool endpoints.
"""

import sys
import json
import logging
import asyncio
from typing import Optional

logger = logging.getLogger(__name__)


async def publish_gossip_state_periodic(bridge, interval: float = 10.0):
    """
    Every `interval` seconds, snapshot gossip node state and publish to Redis.

    Args:
        bridge: NodeEventBridge instance
        interval: Seconds between publishes (default 10)
    """
    logger.info(f"Gossip state publisher started (interval={interval}s)")

    while True:
        try:
            gossip_node = getattr(sys.modules.get("__main__", None), "gossip_node", None)
            if gossip_node:
                state = {
                    "gossip": {
                        "running": True,
                        "node_id": gossip_node.node_id,
                        "port": gossip_node.gossip_port,
                        "is_bootstrap": gossip_node.is_bootstrap,
                        "dht_peers": len(gossip_node.dht_peers),
                        "client_peers": len(gossip_node.client_peers),
                        "total_peers": len(gossip_node.dht_peers) + len(gossip_node.client_peers),
                        "dht_peer_list": [
                            list(p) if isinstance(p, tuple) else p
                            for p in gossip_node.dht_peers
                        ],
                        "client_peer_list": [
                            list(p) if isinstance(p, tuple) else p
                            for p in gossip_node.client_peers
                        ],
                        "synced_peers": len(gossip_node.synced_peers),
                        "synced_peers_list": list(gossip_node.synced_peers),
                        "failed_peers": {str(k): v for k, v in gossip_node.failed_peers.items()},
                        "peer_info": {str(k): v for k, v in getattr(gossip_node, 'peer_info', {}).items()},
                    },
                    "peers": {
                        "node_id": gossip_node.node_id,
                        "is_bootstrap": gossip_node.is_bootstrap,
                        "dht_peers": {
                            "count": len(gossip_node.dht_peers),
                            "peers": [{"host": p[0], "port": p[1]} for p in gossip_node.dht_peers],
                        },
                        "client_peers": {
                            "count": len(gossip_node.client_peers),
                            "peers": [{"host": p[0], "port": p[1]} for p in gossip_node.client_peers],
                        },
                        "synced_peers": {
                            "count": len(gossip_node.synced_peers),
                            "peers": list(gossip_node.synced_peers),
                        },
                        "failed_peers": {str(k): v for k, v in gossip_node.failed_peers.items()},
                        "total_active_peers": len(gossip_node.dht_peers) + len(gossip_node.client_peers),
                    },
                }

                # DHT task status
                dht_task = getattr(sys.modules.get("__main__", None), "dht_task", None)
                if dht_task:
                    state["dht"] = {
                        "running": not dht_task.done(),
                        "task_state": "done" if dht_task.done() else "running",
                    }
                    if dht_task.done() and dht_task.exception():
                        state["dht"]["error"] = str(dht_task.exception())
                else:
                    state["dht"] = {"running": False}

                await bridge.publish_gossip_state(state)

        except asyncio.CancelledError:
            logger.info("Gossip state publisher cancelled")
            break
        except Exception as e:
            logger.warning(f"Error publishing gossip state: {e}")

        await asyncio.sleep(interval)


async def publish_mempool_stats_periodic(bridge, interval: float = 5.0):
    """
    Every `interval` seconds, publish mempool stats to Redis.

    Args:
        bridge: NodeEventBridge instance
        interval: Seconds between publishes (default 5)
    """
    logger.info(f"Mempool stats publisher started (interval={interval}s)")

    while True:
        try:
            from state.state import mempool_manager
            if mempool_manager:
                stats = mempool_manager.get_stats()
                all_txs = mempool_manager.get_all_transactions()
                data = {
                    "size": len(all_txs) if all_txs else 0,
                    "transactions": list(all_txs.keys()) if all_txs else [],
                    "stats": stats,
                }
                client = await bridge._get_pub_client()
                await client.set("qbtc:mempool_stats", json.dumps(data), ex=15)
        except asyncio.CancelledError:
            logger.info("Mempool stats publisher cancelled")
            break
        except Exception as e:
            logger.warning(f"Error publishing mempool stats: {e}")

        await asyncio.sleep(interval)


async def listen_commit_submissions(bridge):
    """
    Listen for commitment write requests from the web process.
    The web process cannot write to the secondary DB, so it forwards
    commitment writes to the node via Redis.
    """
    import redis.asyncio as aioredis

    client = aioredis.from_url(bridge.redis_url, decode_responses=True)
    logger.info("Commitment submission listener started")

    while True:
        try:
            result = await client.brpop("qbtc:commit_submit", timeout=1)
            if result is None:
                continue

            _key, raw = result
            commitment_data = json.loads(raw)

            try:
                from database.database import get_db
                from rocksdict import WriteBatch

                db = get_db()
                btc_address = commitment_data.get("btc_address")
                qbtc_address = commitment_data.get("qbtc_address")
                commitment_hash = commitment_data.get("commitment_hash")

                commitment_key = f"commitment:{btc_address}".encode()
                reverse_key = f"commitment_reverse:{qbtc_address}".encode()

                batch = WriteBatch()
                batch.put(commitment_key, json.dumps(commitment_data).encode())
                batch.put(reverse_key, btc_address.encode())
                db.write(batch)

                logger.info(f"Commitment written via bridge: {btc_address} -> {qbtc_address}")

                # Push success result
                result_key = f"qbtc:commit_result:{commitment_hash}"
                await client.lpush(result_key, json.dumps({"status": "success"}))
                await client.expire(result_key, 30)

            except Exception as e:
                logger.error(f"Error writing commitment: {e}")
                commitment_hash = commitment_data.get("commitment_hash", "unknown")
                result_key = f"qbtc:commit_result:{commitment_hash}"
                await client.lpush(result_key, json.dumps({"status": "error", "error": str(e)}))
                await client.expire(result_key, 30)

        except asyncio.CancelledError:
            logger.info("Commitment listener cancelled")
            break
        except Exception as e:
            logger.error(f"Error in commitment listener: {e}")
            await asyncio.sleep(0.5)

    await client.aclose()
