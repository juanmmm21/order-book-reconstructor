from order_book_reconstructor.models import ReconnectPolicy
from order_book_reconstructor.reconnect import compute_backoff_delay


def test_backoff_grows_with_attempts() -> None:
    policy = ReconnectPolicy(initial_delay_seconds=1.0, max_delay_seconds=30.0, jitter_ratio=0.0)

    assert compute_backoff_delay(1, policy) == 1.0
    assert compute_backoff_delay(2, policy) == 2.0
    assert compute_backoff_delay(3, policy) == 4.0
