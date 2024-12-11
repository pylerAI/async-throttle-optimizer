from typing import Dict, List, Optional, Tuple

from async_throttle_optimizer.throttler import RequestThrottler


class RateFinder:
    """
    A class to find the optimal request rate by testing different rates
    and using a retry logic to adjust the rate until a suitable threshold is met.

    This is a simple example using binary search logic:
    - Start with a range [min_rate, max_rate]
    - Pick a mid rate and run RequestThrottler.
    - If average latency > threshold, lower the max_rate.
    - Else raise the min_rate.
    - Repeat until the difference between min_rate and max_rate is small enough.
    """

    def __init__(
        self,
        urls: List[str],
        min_rate: float = 1.0,
        max_rate: float = 100.0,
        concurrency: int = 5,
        error_threshold: float = 0.1,
        bad_status_threshold: float = 0.1,
        max_iterations: int = 10,
    ) -> None:
        """
        Args:
            urls (List[str]): A list of URLs to fetch.
            min_rate (float): The minimum request rate to start with.
            max_rate (float): The maximum request rate to start with.
            concurrency (int): The number of concurrent requests to use.
            max_iterations (int): The maximum number of iterations to find a suitable rate.
        """
        self.urls = urls
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.concurrency = concurrency
        self.error_threshold = error_threshold
        self.bad_status_threshold = bad_status_threshold
        self.max_iterations = max_iterations
        self.best_rate = None
        self.best_stats = (None, None, None, None)

    def _evaluate_conditions(
        self,
        mean: Optional[float],
        latency_threshold: float,
        error_rate: float,
        status_distrubution: Dict[int, int],
        total_requests: int,
    ) -> bool:
        """
        Evaluate the given metrics and decide if we should lower the rate.
        Return False if conditions are bad (i.e., should decrease the rate), True otherwise.
        """

        # Check latency
        if mean is not None and mean > latency_threshold:
            return False

        # Check error rate
        if error_rate > self.error_threshold:
            return False

        # Check bad status codes
        # Let's define "bad" status as 429 or 500
        bad_counts = 0
        for code in [429, 500]:
            bad_counts += status_distrubution.get(code, 0)
        bad_rate = (bad_counts / total_requests) if total_requests > 0 else 0.0
        if bad_rate > self.bad_status_threshold:
            return False

        return True

    async def find_rate(
        self,
    ) -> Tuple[
        Optional[float],
        Tuple[Optional[float], Optional[float], Optional[float], Optional[float]],
    ]:
        """
        Attempt to find the optimal rate using a binary search approach.
        Returns:
            (best_rate, (mean, std, min_latency, max_latency))
        """
        for i in range(self.max_iterations):
            current_rate = (self.min_rate + self.max_rate) / 2

            # Calculate the latency threshold with concurrency and current rate
            latency_threshold = self.concurrency / current_rate
            print(f"Testing rate: {current_rate:.2f} req/s")

            throttler = RequestThrottler(
                self.urls, rate=current_rate, concurrency=self.concurrency
            )
            (
                total_reqs,
                mean,
                std,
                lat_min,
                lat_max,
                error_rate,
                status_dist,
            ) = await throttler.run()

            if mean is None:
                # No requests completed, possibly too high or network issues
                # Lower the max_rate to be more conservative
                self.max_rate = current_rate
                continue

            print(
                f"Rate {current_rate:.2f} results - "
                f"Avg: {mean:.4f}, Std: {std:.4f}, "
                f"Min: {lat_min:.4f}, Max: {lat_max:.4f}"
            )

            # Evaluate conditions
            if self._evaluate_conditions(
                mean, latency_threshold, error_rate, status_dist, total_reqs
            ):
                # Conditions are good, update the best rate
                print(
                    f"Conditions are acceptable, updating best rate to {current_rate:.2f}"
                )
                self.min_rate = current_rate
                self.best_rate = current_rate
                self.best_stats = (mean, std, lat_min, lat_max, error_rate, status_dist)

            else:
                # Conditions are bad, current rate is too high
                print(
                    f"Conditions are bad, current rate {current_rate:.2f} is too high"
                )
                self.max_rate = current_rate

        return self.best_rate, self.best_stats
