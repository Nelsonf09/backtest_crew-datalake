from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from ib_insync import IB

@dataclass
class IBClientConfig:
    host: str = '127.0.0.1'
    port: int = 7497
    client_id: int = 1011
    timeout: float = 30.0
    rate_limit_rps: float = 0.7  # <=1 req/s

class IBClient:
    def __init__(self, cfg: Optional[IBClientConfig] = None) -> None:
        self.cfg = cfg or IBClientConfig()
        self.ib = IB()
        self._last_ts = 0.0

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def connect(self) -> None:
        self.ib.connect(self.cfg.host, self.cfg.port, clientId=self.cfg.client_id, timeout=self.cfg.timeout)
        _ = self.ib.reqCurrentTime()  # ping

    def disconnect(self) -> None:
        if self.ib.isConnected():
            self.ib.disconnect()

    def _throttle(self) -> None:
        min_interval = 1.0 / max(self.cfg.rate_limit_rps, 0.01)
        now = time.time()
        delay = self._last_ts + min_interval - now
        if delay > 0:
            time.sleep(delay)
        self._last_ts = time.time()
