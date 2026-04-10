from __future__ import annotations

import asyncio
import json
from typing import Dict, List, Tuple


class LiveUpdateBroker:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=25)
        async with self._lock:
            self._subscribers.add(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue) -> None:
        async with self._lock:
            self._subscribers.discard(queue)

    async def publish(self, event: str, data: Dict) -> None:
        message = (event, json.dumps(data, ensure_ascii=True))
        async with self._lock:
            subscribers: List[asyncio.Queue] = list(self._subscribers)

        for queue in subscribers:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                continue


def format_sse(event: str, payload: str) -> str:
    return f"event: {event}\ndata: {payload}\n\n"


def next_company_batch(
    company_names: List[str], start_index: int, batch_size: int
) -> Tuple[List[str], int]:
    if not company_names:
        return [], 0

    if batch_size <= 0 or batch_size >= len(company_names):
        return list(company_names), 0

    end_index = start_index + batch_size
    if end_index <= len(company_names):
        return company_names[start_index:end_index], end_index % len(company_names)

    overflow = end_index - len(company_names)
    batch = company_names[start_index:] + company_names[:overflow]
    return batch, overflow
