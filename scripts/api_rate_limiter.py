"""
APIアクセス間隔を制御するモジュール
"""

import logging
import random
import threading
import time
from collections import defaultdict
from datetime import datetime

# ロギング設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ApiRateLimiter")


class ApiRateLimiter:
    """
    APIアクセス間隔を制御するクラス

    複数のAPIエンドポイントへのアクセスを適切な間隔で制御し、
    サーバー負荷を軽減したり、レート制限に引っかからないようにする。
    """

    def __init__(self, default_rate_limit=1.0, jitter=0.2, config=None):
        """
        初期化

        Args:
            default_rate_limit (float): デフォルトのアクセス間隔（秒）
            jitter (float): ランダム化の揺らぎ幅（0-1の比率）
            config (KeirinConfig, optional): 設定オブジェクト
        """
        # 設定オブジェクトがあれば、そこから設定を読み込む
        if config:
            try:
                rate_limiter_config = config.get_rate_limiter_config()
                default_rate_limit = rate_limiter_config["default_rate_limit"]
                jitter = rate_limiter_config["jitter"]
            except Exception as e:
                logger.warning(f"設定読み込みエラー: {e}")
                logger.warning("デフォルト値を使用します")

        self.default_rate_limit = default_rate_limit
        self.jitter = jitter
        self.last_access_times = defaultdict(lambda: datetime.min)
        self.rate_limits = defaultdict(lambda: default_rate_limit)
        self.lock = threading.RLock()

    def set_rate_limit(self, endpoint, rate_limit):
        """
        特定のエンドポイントのアクセス間隔を設定

        Args:
            endpoint (str): エンドポイント名
            rate_limit (float): アクセス間隔（秒）
        """
        with self.lock:
            self.rate_limits[endpoint] = rate_limit
            logger.debug(
                f"エンドポイント '{endpoint}' のレート制限を {rate_limit}秒に設定しました"
            )

    def wait(self, endpoint=None):
        """
        次のAPIアクセスまで待機

        Args:
            endpoint (str, optional): エンドポイント名、Noneの場合はデフォルト

        Returns:
            float: 実際に待機した秒数
        """
        with self.lock:
            now = datetime.now()
            endpoint_key = endpoint if endpoint else "default"
            rate_limit = self.rate_limits[endpoint_key]

            # 最後のアクセスからの経過時間を計算
            last_access = self.last_access_times[endpoint_key]
            time_since_last = (now - last_access).total_seconds()

            # 次のアクセスまで待つべき時間を計算（ジッター含む）
            jitter_amount = rate_limit * self.jitter
            wait_time = rate_limit + random.uniform(-jitter_amount, jitter_amount)
            wait_time = max(0, wait_time - time_since_last)

            if wait_time > 0:
                logger.debug(
                    f"エンドポイント '{endpoint_key}' のアクセス間隔 {wait_time:.2f}秒を待機中..."
                )
                time.sleep(wait_time)

            # 最終アクセス時間を更新
            self.last_access_times[endpoint_key] = datetime.now()

            return wait_time

    def execute(self, func, endpoint=None, *args, **kwargs):
        """
        APIアクセス関数を実行（適切な待機時間後）

        Args:
            func (callable): 実行する関数
            endpoint (str, optional): エンドポイント名、Noneの場合はデフォルト
            *args, **kwargs: 関数に渡す引数

        Returns:
            any: 関数の戻り値
        """
        self.wait(endpoint)
        return func(*args, **kwargs)

    def get_stats(self):
        """
        現在の統計情報を取得

        Returns:
            dict: 各エンドポイントのアクセス間隔と最終アクセス時間
        """
        with self.lock:
            stats = {
                "rate_limits": dict(self.rate_limits),
                "last_access_times": {
                    endpoint: timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    for endpoint, timestamp in self.last_access_times.items()
                    if timestamp > datetime.min
                },
            }
            return stats


class ApiBackoff:
    """
    APIアクセスのバックオフ戦略を実装するクラス

    API呼び出しが失敗した場合に、再試行間隔を指数関数的に増加させる。
    これによりサーバー負荷を抑えつつ、一時的な障害からの回復を試みる。
    """

    def __init__(
        self,
        initial_delay=1.0,
        max_delay=60.0,
        max_retries=5,
        backoff_factor=2.0,
        config=None,
    ):
        """
        初期化

        Args:
            initial_delay (float): 初期待機時間（秒）
            max_delay (float): 最大待機時間（秒）
            max_retries (int): 最大再試行回数
            backoff_factor (float): バックオフ係数（待機時間の増加倍率）
            config (KeirinConfig, optional): 設定オブジェクト
        """
        # 設定オブジェクトがあれば、そこから設定を読み込む
        if config:
            try:
                backoff_config = config.get_backoff_config()
                initial_delay = backoff_config["initial_delay"]
                max_delay = backoff_config["max_delay"]
                max_retries = backoff_config["max_retries"]
                backoff_factor = backoff_config["backoff_factor"]
            except Exception as e:
                logger.warning(f"設定読み込みエラー: {e}")
                logger.warning("デフォルト値を使用します")

        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_counts = defaultdict(int)
        self.lock = threading.RLock()

    def reset(self, endpoint=None):
        """
        エンドポイントの再試行カウントをリセット

        Args:
            endpoint (str, optional): エンドポイント名、Noneの場合は全て
        """
        with self.lock:
            if endpoint:
                self.retry_counts[endpoint] = 0
            else:
                self.retry_counts.clear()

    def should_retry(self, endpoint=None):
        """
        再試行すべきかどうかを判断

        Args:
            endpoint (str, optional): エンドポイント名、Noneの場合はデフォルト

        Returns:
            bool: 再試行すべきならTrue
        """
        with self.lock:
            endpoint_key = endpoint if endpoint else "default"
            return self.retry_counts[endpoint_key] < self.max_retries

    def wait_before_retry(self, endpoint=None):
        """
        再試行前に待機

        Args:
            endpoint (str, optional): エンドポイント名、Noneの場合はデフォルト

        Returns:
            float: 待機した秒数
        """
        with self.lock:
            endpoint_key = endpoint if endpoint else "default"
            retry_count = self.retry_counts[endpoint_key]

            # 待機時間を計算（指数バックオフ）
            delay = min(
                self.initial_delay * (self.backoff_factor**retry_count), self.max_delay
            )

            # ジッターを追加（±10%）
            jitter = random.uniform(-0.1, 0.1) * delay
            delay += jitter

            logger.info(
                f"エンドポイント '{endpoint_key}' の再試行 #{retry_count+1}, {delay:.2f}秒待機中..."
            )
            time.sleep(delay)

            # 再試行カウントを増加
            self.retry_counts[endpoint_key] += 1

            return delay

    def execute_with_retry(self, func, endpoint=None, *args, **kwargs):
        """
        再試行ロジック付きで関数を実行

        Args:
            func (callable): 実行する関数
            endpoint (str, optional): エンドポイント名、Noneの場合はデフォルト
            *args, **kwargs: 関数に渡す引数

        Returns:
            any: 関数の戻り値

        Raises:
            Exception: 全ての再試行が失敗した場合
        """
        endpoint_key = endpoint if endpoint else "default"
        self.reset(endpoint_key)

        last_error = None
        while self.should_retry(endpoint_key):
            try:
                result = func(*args, **kwargs)
                self.reset(endpoint_key)  # 成功したらカウントをリセット
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"エンドポイント '{endpoint_key}' でエラー発生: {e}")
                if not self.should_retry(endpoint_key):
                    break
                self.wait_before_retry(endpoint_key)

        # 全ての再試行が失敗
        logger.error(f"エンドポイント '{endpoint_key}' の全再試行が失敗しました")
        raise last_error


# 設定ファイルから初期化するヘルパー関数
def create_rate_limiter_from_config(config):
    """
    設定ファイルからApiRateLimiterを作成

    Args:
        config: KeirinConfigオブジェクト

    Returns:
        ApiRateLimiter: 設定に基づくレート制限オブジェクト
    """
    return ApiRateLimiter(config=config)


def create_backoff_from_config(config):
    """
    設定ファイルからApiBackoffを作成

    Args:
        config: KeirinConfigオブジェクト

    Returns:
        ApiBackoff: 設定に基づくバックオフオブジェクト
    """
    return ApiBackoff(config=config)


# 使用例
if __name__ == "__main__":
    import requests

    try:
        # 設定ファイルからインポート
        from keirin_config import get_config

        config = get_config()

        # 設定からレート制限を作成
        rate_limiter = create_rate_limiter_from_config(config)

        # エンドポイント設定
        rate_limiter.set_rate_limit(
            "winticket_api", config.getfloat("WINTICKET", "default_rate_limit")
        )
        rate_limiter.set_rate_limit(
            "yenjoy_api", config.getfloat("YENJOY", "default_rate_limit")
        )

        def call_api(url):
            logger.info(f"APIコール: {url}")
            return requests.get(url)

        # 通常の待機
        print("通常のAPIコール（待機あり）:")
        rate_limiter.wait("winticket_api")
        call_api("https://example.com/api1")

        rate_limiter.wait("winticket_api")
        call_api("https://example.com/api1")

        # 関数ラッパーの使用
        print("\n関数ラッパーの使用:")
        result = rate_limiter.execute(
            call_api, "yenjoy_api", "https://example.com/api2"
        )

        # バックオフの使用例
        print("\nバックオフ戦略の使用:")
        backoff = create_backoff_from_config(config)

        def failing_api():
            """失敗するAPI呼び出しのシミュレーション"""
            logger.info("APIコール試行中...")
            raise RuntimeError("API一時的にエラー")

        try:
            backoff.execute_with_retry(failing_api, "test_api")
        except Exception as e:
            print(f"最終エラー: {e}")

    except ImportError:
        # 設定ファイルがない場合はデフォルト値で実行
        print("設定ファイルがないためデフォルト値で実行します")
        rate_limiter = ApiRateLimiter(default_rate_limit=2.0)
        rate_limiter.set_rate_limit("winticket_api", 3.0)
        rate_limiter.set_rate_limit("yenjoy_api", 5.0)

        def call_api(url):
            logger.info(f"APIコール: {url}")
            return requests.get(url)

        # 通常の待機
        print("通常のAPIコール（待機あり）:")
        rate_limiter.wait("winticket_api")
        call_api("https://example.com/api1")

        # バックオフの使用例
        print("\nバックオフ戦略の使用:")
        backoff = ApiBackoff(initial_delay=1.0, max_retries=3)

        def failing_api():
            """失敗するAPI呼び出しのシミュレーション"""
            logger.info("APIコール試行中...")
            raise RuntimeError("API一時的にエラー")

        try:
            backoff.execute_with_retry(failing_api, "test_api")
        except Exception as e:
            print(f"最終エラー: {e}")
