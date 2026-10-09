import errno

import pytest

from metaflow.plugins.datatools.s3 import s3op

POSIX_SEM_VALUE_MAX = 32767

_MAXSIZES = []


class _CapEnforcingQueue:
    """Queue stub that reproduces the Darwin SEM_VALUE_MAX cap on any platform."""

    def __init__(self, maxsize=0):
        _MAXSIZES.append(maxsize)
        if maxsize > POSIX_SEM_VALUE_MAX:
            raise OSError(errno.EINVAL, "Invalid argument")

    def put(self, item):
        pass


class _FakeProcess:
    """Process stub that creates its (empty) result file and reports success."""

    def __init__(self, args, **_):
        self.exitcode = 0
        open(args[0], "w").close()

    def start(self):
        pass

    def join(self, timeout=None):
        pass


def test_process_urls_batches_input(monkeypatch):
    """process_urls must hand start_workers batches no larger than the cap."""
    calls = []

    def fake_start_workers(mode, urls, num_workers, inject_failure, s3config):
        calls.append(list(urls))
        return list(urls), None

    monkeypatch.setattr(s3op, "start_workers", fake_start_workers)

    urls = list(range(s3op.MAX_URLS_PER_QUEUE * 4 + 3322))
    results, transient_error_type = s3op.process_urls("get", urls, False, 0, 64, None)

    assert results == urls
    assert transient_error_type is None
    assert all(len(batch) <= s3op.MAX_URLS_PER_QUEUE for batch in calls)
    assert [item for batch in calls for item in batch] == urls


def test_process_urls_queue_stays_below_sem_value_max(monkeypatch):
    """Regression: a run larger than POSIX_SEM_VALUE_MAX must not crash on macOS.

    Without batching, start_workers calls Queue(len(urls) + num_workers) with
    36090 + 64, which raises OSError(EINVAL) on Darwin. The cap-enforcing fake
    queue reproduces that limit on any platform.
    """
    _MAXSIZES.clear()
    monkeypatch.setattr(s3op, "Queue", _CapEnforcingQueue)
    monkeypatch.setattr(s3op, "Process", _FakeProcess)

    urls = list(range(36090))
    results, _ = s3op.process_urls("get", urls, False, 0, 64, None)

    assert len(results) == len(urls)
    assert _MAXSIZES
    assert max(_MAXSIZES) <= POSIX_SEM_VALUE_MAX
    assert max(_MAXSIZES) <= 2 * s3op.MAX_URLS_PER_QUEUE


def test_queue_above_sem_value_max_would_fail():
    """Documents the failure mode the batching fix avoids."""
    with pytest.raises(OSError):
        _CapEnforcingQueue(POSIX_SEM_VALUE_MAX + 1)
