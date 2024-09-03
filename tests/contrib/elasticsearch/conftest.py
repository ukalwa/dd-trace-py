import http.client
import logging
import os
import time

from ..config import ELASTICSEARCH_CONFIG
from ..config import OPENSEARCH_CONFIG


log = logging.getLogger(__name__)


def wait_for_elasticsearch(host: str, port: int, interval: int = 2, retries: int = 10):
    for _ in range(retries):
        log.info("Waiting for Elasticsearch to be ready at %s:%s", host, port)
        conn = http.client.HTTPConnection(host, port)
        try:
            conn.request("GET", "/")
            response = conn.getresponse()
            if response.status == 200:
                return True
        except Exception:
            pass
        finally:
            conn.close()

        log.info("Retrying in %s seconds...", interval)
        time.sleep(interval)
    else:
        raise TimeoutError("Elasticsearch did not start in time")


if os.getenv("CI") == "1":
    wait_for_elasticsearch(ELASTICSEARCH_CONFIG["host"], ELASTICSEARCH_CONFIG["port"])
    wait_for_elasticsearch(OPENSEARCH_CONFIG["host"], OPENSEARCH_CONFIG["port"])
