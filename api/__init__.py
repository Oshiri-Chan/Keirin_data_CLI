"""
APIパッケージ
"""

from .api_rate_limiter import APIRateLimiter as _APIRateLimiter
from .keirin_mappings import KeirinMappings
from .winticket import (
    WinticketStep1API,
    WinticketStep2API,
    WinticketStep3API,
    WinticketStep4API,
    WinticketStep5API,
)
from .winticket_api import WinticketAPI
from .yenjoy import YenjoyStep5API
from .yenjoy_api import YenjoyAPI

# 両方のクラス名をサポート（ApiRateLimiterとAPIRateLimiter）
APIRateLimiter = _APIRateLimiter
ApiRateLimiter = _APIRateLimiter

__all__ = [
    "WinticketAPI",
    "YenjoyAPI",
    "APIRateLimiter",
    "ApiRateLimiter",  # 後方互換性のために追加
    "KeirinMappings",
    # 分割したステップAPI
    "WinticketStep1API",
    "WinticketStep2API",
    "WinticketStep3API",
    "WinticketStep4API",
    "WinticketStep5API",
    "YenjoyStep5API",
]
