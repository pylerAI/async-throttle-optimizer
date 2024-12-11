import asyncio
import random
import time
from collections import deque

import pytest
from aiohttp import web

from src.async_throttle_optimizer.throttler import RequestThrottler

# Gaussian distribution parameters for latency
LATENCY_MEAN = 0.1  # seconds
LATENCY_STD = 0.05  # seconds
MIN_LATENCY = 0.01  # seconds (minimum latency)
MAX_LATENCY = 0.5  # seconds (maximum latency)

# throttling parameters
WINDOW_SIZE = 10  # seconds
MAX_REQUESTS_IN_WINDOW = 100  # maximum requests in the window

request_times = deque()


async def time_based_throttling_handler(request):
    current_time = time.time()

    # Add current time to the request_times queue
    request_times.append(current_time)

    # Remove old requests from the queue
    while request_times and request_times[0] < current_time - WINDOW_SIZE:
        request_times.popleft()

    # Current number of requests in the WINDOW_SIZE
    current_request_count = len(request_times)

    if current_request_count >= MAX_REQUESTS_IN_WINDOW:
        # Satisfy the throttling condition
        return web.Response(status=429, text="Too many requests")

    # Simulate request latency
    latency = max(
        MIN_LATENCY, min(random.gauss(LATENCY_MEAN, LATENCY_STD), MAX_LATENCY)
    )
    await asyncio.sleep(latency)

    return web.Response(
        status=200, text="Request processed", headers={"X-Server-Load": "3.5"}
    )


@pytest.fixture
async def test_server(aiohttp_client):
    app = web.Application()
    app.router.add_get("/test", time_based_throttling_handler)
    client = await aiohttp_client(app)
    return client


@pytest.mark.asyncio
async def test_throttler_with_time_based_429(test_server):
    base_url = test_server.make_url("/test")

    # 50 requests, 429 error when exceed the limit
    urls = [base_url for _ in range(50)]

    # high rate request -> 429 error
    # example: 20 req/s: 20 requests per second -> 429 error when set the limit to 10 req/s
    throttler = RequestThrottler(urls, rate=10, concurrency=5)
    (
        total_reqs,
        mean,
        std,
        lat_min,
        lat_max,
        error_rate,
        status_dist,
    ) = await throttler.run()

    assert total_reqs == 50
    assert mean is not None
    assert std is not None
    assert lat_min is not None
    assert lat_max is not None
    assert error_rate is not None
    assert status_dist is not None
