import atexit
import datetime
import logging
import threading
from collections import Counter, namedtuple
from typing import Any, Dict

from localstack import config
from localstack.utils import analytics
from localstack.utils.scheduler import Scheduler

LOG = logging.getLogger(__name__)
FLUSH_INTERVAL_SECS = 10


ResponseInfo = namedtuple("ResponseInfo", "service, operation, status_code")


class ResponseAggregator:
    """
    Collects HTTP response data, aggregates it into small batches, and periodically emits (flushes) it as an analytics event
    """

    def __init__(self):
        self.response_counter = Counter()
        self.period_start_time = datetime.datetime.utcnow()
        self.flush_scheduler = Scheduler()
        self.scheduler_thread = threading.Thread(target=self.flush_scheduler.run)
        self.scheduler_thread.start()
        self.flush_scheduler.schedule(func=self._flush, period=FLUSH_INTERVAL_SECS, fixed_rate=True)
        atexit.register(self._flush)

    def add_response(self, service_name: str, operation_name: str, response_code: int):
        """
        Add an HTTP response for aggregation and collection
        :param service_name: name of the service the request was aimed at, e.g. s3
        :param operation_name: name of the operation, e.g. CreateBucket
        :param response_code: HTTP status code of the response, e.g. 200
        """
        if config.DISABLE_EVENTS:
            return

        response_info = ResponseInfo(
            service=service_name,
            operation=operation_name,
            status_code=response_code,
        )
        self.response_counter[response_info] += 1

    def _get_analytics_payload(self) -> Dict[str, Any]:
        return {
            "period_start_time": self.period_start_time.isoformat() + "Z",
            "period_end_time": datetime.datetime.utcnow().isoformat() + "Z",
            "http_response_aggregations": [
                {**resp._asdict(), "count": count} for resp, count in self.response_counter.items()
            ],
        }

    def _flush(self):
        """
        Flushes the current batch of HTTP response data as an analytics event.
        This happens automatically in the background.
        """
        if len(self.response_counter) > 0:
            analytics_payload = self._get_analytics_payload()
            analytics.log.event("http_response_agg", analytics_payload)
            self.response_counter.clear()
        self.period_start_time = datetime.datetime.utcnow()