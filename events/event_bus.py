"""
Event Bus for real-time WebSocket notifications.

Uses a PriorityQueue so user-facing events (transactions, wallet balance)
are dispatched before infrastructure events (peers, blocks).
"""

import asyncio
import itertools
import logging
from typing import Dict, List, Callable, Any
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum

logger = logging.getLogger(__name__)


class EventPriority(IntEnum):
    """Lower value = higher priority."""
    HIGH = 0    # User-facing: transactions, wallet balance
    NORMAL = 5  # Block events, UTXO updates
    LOW = 10    # Network / peer events


@dataclass
class Event:
    """Event data structure"""
    type: str
    data: Dict[str, Any]
    timestamp: float
    source: str = "system"


@dataclass(order=True)
class PrioritizedEvent:
    """Wrapper that makes Event sortable for PriorityQueue.
    Orders by (priority, sequence) so FIFO is preserved within the same priority.
    """
    priority: int
    sequence: int
    event: Event = field(compare=False)


# Event types
class EventTypes:
    """Standard event types"""
    # Transaction events
    TRANSACTION_PENDING = "transaction_pending"
    TRANSACTION_CONFIRMED = "transaction_confirmed"
    TRANSACTION_FAILED = "transaction_failed"

    # Block events
    BLOCK_ADDED = "block_added"
    BLOCK_VALIDATED = "block_validated"

    # UTXO events
    UTXO_CREATED = "utxo_created"
    UTXO_SPENT = "utxo_spent"

    # Wallet events
    WALLET_BALANCE_CHANGED = "wallet_balance_changed"

    # Network events
    PEER_CONNECTED = "peer_connected"
    PEER_DISCONNECTED = "peer_disconnected"

    # Bridge events
    BRIDGE_DEPOSIT = "bridge_deposit"
    BRIDGE_WITHDRAWAL = "bridge_withdrawal"

    # Custom events
    CUSTOM = "custom"


# Priority mapping — unlisted event types default to NORMAL
EVENT_PRIORITIES: Dict[str, EventPriority] = {
    EventTypes.TRANSACTION_CONFIRMED: EventPriority.HIGH,
    EventTypes.TRANSACTION_PENDING:   EventPriority.HIGH,
    EventTypes.WALLET_BALANCE_CHANGED: EventPriority.HIGH,

    EventTypes.BLOCK_ADDED:     EventPriority.NORMAL,
    EventTypes.BLOCK_VALIDATED: EventPriority.NORMAL,
    EventTypes.UTXO_CREATED:    EventPriority.NORMAL,
    EventTypes.UTXO_SPENT:      EventPriority.NORMAL,

    EventTypes.PEER_CONNECTED:    EventPriority.LOW,
    EventTypes.PEER_DISCONNECTED: EventPriority.LOW,
}


class EventBus:
    """
    Centralized event bus for broadcasting state changes to WebSocket clients.
    Uses a PriorityQueue so user-facing events are processed first.
    """

    def __init__(self):
        self.listeners: Dict[str, List[Callable]] = defaultdict(list)
        self.event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._sequence = itertools.count()  # monotonic counter for FIFO within priority
        self.running = False
        self.processor_task = None
        logger.info("EventBus initialized (priority queue)")

    async def start(self):
        """Start the event processor"""
        if not self.running:
            self.running = True
            self.processor_task = asyncio.create_task(self._process_events())
            logger.info("EventBus started")

    async def stop(self):
        """Stop the event processor"""
        self.running = False
        if self.processor_task:
            await self.processor_task
            logger.info("EventBus stopped")

    async def _process_events(self):
        """Process events from the priority queue"""
        while self.running:
            try:
                # Wait for events with timeout to allow checking running status
                prioritized = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                await self._dispatch_event(prioritized.event)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing event: {e}")

    async def _dispatch_event(self, event: Event):
        """Dispatch event to all registered listeners"""
        listeners = self.listeners.get(event.type, [])

        if not listeners:
            logger.debug(f"No listeners for event type: {event.type}")
            return

        logger.debug(f"Dispatching {event.type} to {len(listeners)} listeners")

        # Create tasks for all listeners to run concurrently
        tasks = []
        for listener in listeners:
            task = asyncio.create_task(self._call_listener(listener, event))
            tasks.append(task)

        # Wait for all listeners to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any errors
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Listener {listeners[i].__name__} failed: {result}")

    async def _call_listener(self, listener: Callable, event: Event):
        """Call a listener with error handling"""
        try:
            await listener(event)
        except Exception as e:
            logger.error(f"Error in listener {listener.__name__}: {e}")
            raise

    def subscribe(self, event_type: str, listener: Callable):
        """Subscribe to an event type"""
        self.listeners[event_type].append(listener)
        logger.info(f"Subscribed {listener.__name__} to {event_type}")

    def unsubscribe(self, event_type: str, listener: Callable):
        """Unsubscribe from an event type"""
        if listener in self.listeners[event_type]:
            self.listeners[event_type].remove(listener)
            logger.info(f"Unsubscribed {listener.__name__} from {event_type}")

    async def emit(self, event_type: str, data: Dict[str, Any], source: str = "system"):
        """Emit an event (automatically prioritized)"""
        event = Event(
            type=event_type,
            data=data,
            timestamp=datetime.now().timestamp(),
            source=source
        )

        priority = EVENT_PRIORITIES.get(event_type, EventPriority.NORMAL)
        prioritized = PrioritizedEvent(
            priority=int(priority),
            sequence=next(self._sequence),
            event=event
        )

        await self.event_queue.put(prioritized)
        logger.debug(f"Emitted event: {event_type} from {source} (priority={priority.name})")


# Global event bus instance
event_bus = EventBus()
