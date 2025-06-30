"""
APIリクエストのレート制限とバックオフを管理するモジュール
"""

import logging
import random
import time
from datetime import datetime


class APIRateLimiter:
    """
    APIリクエストのレート制限を管理するクラス

    特定のAPIに対するリクエスト送信のタイミングを制御して、
    サーバーへの負荷を抑えるための機能を提供します。
    """

    def __init__(self, default_rate=1.0, jitter=0.2, logger=None):
        """
        初期化

        Args:
            default_rate (float): デフォルトのリクエスト間隔（秒）
            jitter (float): リクエスト間隔にランダム性を持たせる割合（0.0-1.0）
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.default_rate = default_rate
        self.jitter = max(0.0, min(1.0, jitter))  # 0.0-1.0の範囲に制限
        self.last_request_time = {}
        self.logger = logger or logging.getLogger(__name__)

    def wait(self, endpoint=None, rate=None):
        """
        次のリクエストを送信するまで待機

        Args:
            endpoint (str, optional): エンドポイント名（省略時はdefault）
            rate (float, optional): リクエスト間隔（秒、省略時はデフォルト値）
        """
        endpoint = endpoint or "default"
        rate = rate or self.default_rate

        # 現在時刻
        now = datetime.now()

        # 最後のリクエスト時刻を取得
        last_time = self.last_request_time.get(endpoint)

        if last_time:
            # ジッター（揺らぎ）を計算
            jitter_amount = rate * self.jitter
            adjusted_rate = rate

            # ジッターが指定されていればランダムな値を追加
            if jitter_amount > 0:
                adjusted_rate += random.uniform(-jitter_amount / 2, jitter_amount / 2)
                # 最小値は保証
                adjusted_rate = max(0.1, adjusted_rate)

            # 最後のリクエストからの経過時間を計算
            elapsed = now - last_time
            sleep_time = adjusted_rate - elapsed.total_seconds()

            # 必要ならば待機
            if sleep_time > 0:
                self.logger.debug(f"APIレート制限: {endpoint} - {sleep_time:.2f}秒待機")
                time.sleep(sleep_time)

        # 最後のリクエスト時刻を更新（現在時刻を取り直す）
        self.last_request_time[endpoint] = datetime.now()


class ApiBackoff:
    """
    APIリクエストのバックオフを管理するクラス

    APIリクエストが失敗した際に、再試行のタイミングを指数関数的に遅延させて
    サーバーへの負荷を抑えるための機能を提供します。
    """

    def __init__(
        self,
        initial_delay=2.0,
        max_delay=60.0,
        max_retries=5,
        backoff_factor=2.0,
        logger=None,
    ):
        """
        初期化

        Args:
            initial_delay (float): 初期待機時間（秒）
            max_delay (float): 最大待機時間（秒）
            max_retries (int): 最大再試行回数
            backoff_factor (float): バックオフ係数（待機時間の増加倍率）
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.logger = logger or logging.getLogger(__name__)
        self.retry_count = {}
        self.last_retry_time = {}

    def reset(self, endpoint=None):
        """
        リトライカウンタをリセット

        Args:
            endpoint (str, optional): エンドポイント名（省略時はdefault）
        """
        endpoint = endpoint or "default"
        if endpoint in self.retry_count:
            del self.retry_count[endpoint]
        if endpoint in self.last_retry_time:
            del self.last_retry_time[endpoint]

    def should_retry(self, endpoint=None):
        """
        リトライすべきかどうかを判断

        Args:
            endpoint (str, optional): エンドポイント名（省略時はdefault）

        Returns:
            bool: リトライすべき場合True
        """
        endpoint = endpoint or "default"
        count = self.retry_count.get(endpoint, 0)
        return count < self.max_retries

    def wait_before_retry(self, endpoint=None):
        """
        リトライ前に待機

        Args:
            endpoint (str, optional): エンドポイント名（省略時はdefault）

        Returns:
            bool: 待機が成功し、リトライできる場合はTrue
        """
        endpoint = endpoint or "default"

        # リトライカウントを取得・更新
        count = self.retry_count.get(endpoint, 0)
        self.retry_count[endpoint] = count + 1

        # 最大リトライ回数をチェック
        if count >= self.max_retries:
            self.logger.warning(
                f"APIリトライ上限到達: {endpoint} - {self.max_retries}回"
            )
            return False

        # バックオフ時間の計算（指数関数的増加）
        delay = min(self.max_delay, self.initial_delay * (self.backoff_factor**count))

        # ジッター（揺らぎ）を追加（±10%）
        jitter = delay * 0.1
        delay = delay + random.uniform(-jitter, jitter)

        self.logger.info(
            f"APIリトライ: {endpoint} - {count+1}/{self.max_retries}回目 ({delay:.2f}秒待機)"
        )

        # 待機
        time.sleep(delay)

        # 最終リトライ時刻を更新
        self.last_retry_time[endpoint] = datetime.now()

        return True
