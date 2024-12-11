import asyncio
import math
import time
from typing import Dict, List, Optional, Tuple

import aiohttp


class RequestThrottler:
    """
    A class to throttle asyncronous HTTP requests and measure latency statistics.

    Attributes:
        urls (List[str]): A list of URLs to fetch.
        rate (float): Number of requests per second.
        concurrency (int): Maximum number of concurrent requests.
        latencies (List[float]): Recorded latencies for completed requests.
    """

    def __init__(
        self, urls: List[str], rate: float = 10.0, concurrency: int = 5
    ) -> None:
        self.urls: List[str] = urls
        self.rate: float = rate
        self.concurrency: int = concurrency
        self.latencies: List[float] = []
        self.success_count: int = 0
        self.error_count: int = 0
        self.status_counts: Dict[int, int] = {}
        self.req_start_timestamps: Dict[str, float] = {}
        self.worker_req_map: Dict[int, List[str]] = {}
        self.req_timeline: Dict[int, List[Tuple[float, float]]] = {}

    async def _token_dispenser(self, token_queue: asyncio.Queue) -> None:
        """
        A coroutine to dispense tokens with a delay.
        """
        delay = 1 / self.rate
        while True:
            await asyncio.sleep(delay)
            await token_queue.put(None)  # issue a token

    async def _send_request(
        self, session: aiohttp.ClientSession, url: str
    ) -> Tuple[float, float]:
        start = time.time()
        try:
            async with session.get(url) as response:
                await response.text()  # wait for full response

                status = response.status
                self.status_counts[status] = self.status_counts.get(status, 0) + 1

        except (aiohttp.ClientError, asyncio.TimeoutError):
            # when request fails, increment error counter
            self.error_count += 1
            return
        end = time.time()

        latency = end - start
        self.latencies.append(latency)
        self.success_count += 1
        self.req_start_timestamps[url] = start
        print(
            f"Completed request to {url} in {latency:.4f} seconds with status {status}"
        )

        return start, end

    async def _worker(
        self,
        worker_id: int,
        queue: asyncio.Queue,
        token_queue: asyncio.Queue,
        start_ref_event: asyncio.Event,
        session: aiohttp.ClientSession,
    ) -> None:
        while True:
            url = await queue.get()

            if self.worker_req_map.get(worker_id) is None:
                self.worker_req_map[worker_id] = [url]
            else:
                self.worker_req_map[worker_id].append(url)

            # wait for a token -> maintain the constant interval globally
            await token_queue.get()
            # utilize the token -> send request
            token_queue.task_done()

            start_time, end_time = await self._send_request(session, url)

            if not start_ref_event.is_set():
                global_start_time = start_time
                start_ref_event.set()
                start_ref_event.global_start_time = global_start_time

            base_time = start_ref_event.global_start_time

            self.req_timeline[worker_id].append(
                (start_time - base_time, end_time - base_time)
            )

            queue.task_done()

    def compute_stats(
        self,
    ) -> Tuple[
        int,
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[float],
        float,
        Dict[int, int],
    ]:
        """
        Compute latency statistics:
        Returns:
            total_requests,mean, std, min_latency, max_latency, error_rate, status_distribution
        """
        total_requests = self.success_count + self.error_count
        if total_requests == 0:
            return 0, None, None, None, None, 0.0, {}

        n = len(self.latencies)
        if n == 0:
            # All requests failed
            mean = std = min_lat = max_lat = None

        else:
            mean = sum(self.latencies) / n
            sum_sq = sum(x * x for x in self.latencies)
            std = math.sqrt((sum_sq / n) - (mean * mean))
            min_lat = min(self.latencies)
            max_lat = max(self.latencies)

        error_rate = self.error_count / total_requests if total_requests > 0 else 0.0
        status_distribution = dict(self.status_counts)

        return (
            total_requests,
            mean,
            std,
            min_lat,
            max_lat,
            error_rate,
            status_distribution,
        )

    async def run(
        self,
    ) -> Tuple[
        Optional[float],
        Optional[float],
        Optional[float],
        Optional[float],
        float,
        Dict[int, int],
    ]:
        """
        Run the throttler and return the computed statistics.
        """
        queue = asyncio.Queue()
        for url in self.urls:
            await queue.put(url)

        token_queue = asyncio.Queue()

        async with aiohttp.ClientSession() as session:
            token_task = asyncio.create_task(self._token_dispenser(token_queue))

            self.req_timeline = {wid: [] for wid in range(self.concurrency)}

            start_ref_event = asyncio.Event()

            workers = [
                asyncio.create_task(
                    self._worker(i, queue, token_queue, start_ref_event, session)
                )
                for i in range(self.concurrency)
            ]

            await queue.join()

            for w in workers:
                w.cancel()

            token_task.cancel()

        return self.compute_stats()
