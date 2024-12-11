# async_throttle_optimizer

A library to optimize the request rate for async requests.

## Installation

```bash
pip install async-throttle-optimizer
```

## Usage

```python
from async_throttle_optimizer.throttler import RequestThrottler

async def main():
    urls = ["https://example.com/api/v1/data"] * 100
    throttler = RequestThrottler(urls, rate=5, concurrency=2)
    await throttler.run()

asyncio.run(main())
```

## License

MIT License
