"""
Winticket API の基底クラス
"""

import json
import logging
import time
import traceback
from datetime import datetime

import requests


class WinticketBaseAPI:
    """
    Winticket APIの基底クラス - 共通機能を提供
    """

    BASE_URL = "https://www.winticket.jp"
    API_URL = "https://api.winticket.jp/v1"

    def __init__(self, logger=None):
        """
        初期化

        Args:
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
            }
        )
        self._initialized = True
        self.logger.info(f"{self.__class__.__name__}が初期化されました")

    def _make_api_request(self, url, retry_count=3, retry_delay=2):
        """
        API リクエストを実行して結果を返す

        Args:
            url (str): リクエストURL
            retry_count (int): リトライ回数
            retry_delay (int): 最初のリトライ待機時間（秒）

        Returns:
            dict: APIレスポンス（JSON）
        """
        for attempt in range(retry_count):
            try:
                self.logger.debug(f"API リクエスト: {url}")

                # リクエスト送信時のデバッグ情報
                request_start_time = datetime.now()
                self.logger.debug(
                    f"リクエスト開始時刻: {request_start_time.strftime('%H:%M:%S.%f')[:-3]}"
                )

                # HTTPリクエストを実行
                response = self.session.get(url, timeout=30)

                # リクエスト完了時のデバッグ情報
                request_end_time = datetime.now()
                request_duration = (
                    request_end_time - request_start_time
                ).total_seconds()
                self.logger.debug(
                    f"リクエスト完了時刻: {request_end_time.strftime('%H:%M:%S.%f')[:-3]}, 所要時間: {request_duration:.3f}秒"
                )

                status_code = response.status_code
                log_message_prefix = "API Status: "
                self.logger.debug(f"{log_message_prefix}{status_code}")

                # エラーチェック
                response.raise_for_status()

                # レスポンスの概要情報をログに出力
                content_length = len(response.content)
                log_message_size = f"APIレスポンスのサイズ: {content_length} バイト"
                self.logger.info(log_message_size)

                # JSONデータのパース
                parse_start_time = datetime.now()
                self.logger.debug(
                    f"JSONパース開始: {parse_start_time.strftime('%H:%M:%S.%f')[:-3]}"
                )

                data = response.json()

                parse_end_time = datetime.now()
                parse_duration = (parse_end_time - parse_start_time).total_seconds()
                self.logger.debug(
                    f"JSONパース完了: {parse_end_time.strftime('%H:%M:%S.%f')[:-3]}, 所要時間: {parse_duration:.3f}秒"
                )

                return data

            except requests.RequestException as e:
                if attempt < retry_count - 1:
                    # リトライ間隔を徐々に増やす（指数バックオフ）
                    wait_time = retry_delay ** (attempt + 1)
                    error_message_str = str(e)
                    log_message_retry_part1 = f"API req fail (retry {attempt+1}/{retry_count}): "  # noqa: E501
                    log_message_retry_part2 = f"{error_message_str[:50]}..."
                    self.logger.warning(
                        f"{log_message_retry_part1}{log_message_retry_part2}"
                    )

                    # 詳細なエラー情報を出力
                    if hasattr(e, "response") and e.response:
                        self.logger.warning(
                            f"HTTP Error: status={e.response.status_code}, body={e.response.text[:50]}..."  # noqa: E501
                        )

                    self.logger.info(f"{wait_time}秒後にリトライします...")
                    time.sleep(wait_time)
                else:
                    error_message = str(e)
                    log_message_max_retry = (
                        f"最大リトライ回数に達しました。API リクエストに失敗: "
                        f"{error_message}"
                    )
                    self.logger.error(log_message_max_retry)
                    return None
            except json.JSONDecodeError as e:
                self.logger.error(f"レスポンスのJSONパースに失敗: {str(e)}")
                if response and hasattr(response, "text"):
                    self.logger.debug(
                        f"受信したレスポンス内容: {response.text[:500]}..."
                    )
                return None
            except Exception as e:
                self.logger.error(f"API リクエスト中に予期せぬエラー: {str(e)}")
                self.logger.debug(f"エラーの詳細: {traceback.format_exc()}")
                return None

    def _get_schedule_id_for_date(self, cup_data, target_date=None):
        """
        日付に基づいて適切なスケジュールIDを取得

        Args:
            cup_data (dict): カップ情報のレスポンスデータ
            target_date (str): 対象日付（YYYYMMDD形式、Noneの場合は最新のスケジュールを返す）

        Returns:
            tuple: (スケジュールID, スケジュールインデックス)
        """
        if "schedules" not in cup_data or not cup_data["schedules"]:
            self.logger.warning("スケジュール情報がありません")
            return None, 0

        schedules = cup_data["schedules"]

        # スケジュールが1つしかない場合はそれを返す
        if len(schedules) == 1:
            return schedules[0]["id"], 0

        # 特定の日付が指定されていない場合は現在開催中または最新のスケジュールを返す
        if not target_date:
            # 最新の日程を使用
            schedule_id = schedules[-1].get("id")
            schedule_index = len(schedules) - 1
            return schedule_id, schedule_index

        # 指定された日付に一致するスケジュールを探す
        for i, schedule in enumerate(schedules):
            if schedule.get("date") == target_date:
                return schedule.get("id"), i

        # 一致するものがなければ最も近い日付のスケジュールを返す
        closest_schedule = min(
            schedules, key=lambda s: abs(int(s.get("date", "0")) - int(target_date))
        )
        schedule_index = schedules.index(closest_schedule)
        return closest_schedule.get("id"), schedule_index

    def _get_nested_keys(self, data, prefix=""):
        """
        ネストされた辞書の全キーを取得する（デバッグ用）

        Args:
            data: 調査対象の辞書/リスト
            prefix: 現在のキーパス

        Returns:
            list: すべてのキーパスのリスト
        """
        keys = []

        if isinstance(data, dict):
            for key, value in data.items():
                current_key = f"{prefix}.{key}" if prefix else key
                keys.append(current_key)

                if isinstance(value, (dict, list)):
                    keys.extend(self._get_nested_keys(value, current_key))
        elif isinstance(data, list) and data:
            # リストの最初の要素のキーを調査
            keys.extend(self._get_nested_keys(data[0], f"{prefix}[0]"))

        return keys

    def _sleep_between_requests(self, seconds=1):
        """
        リクエスト間のスリープ（サーバー負荷を抑えるため）

        Args:
            seconds (int): スリープ秒数
        """
        time.sleep(seconds)

    def cleanup(self):
        """
        リソースのクリーンアップを行う
        """
        if hasattr(self, "session") and self.session:
            try:
                self.session.close()
                self.logger.info(
                    f"{self.__class__.__name__}: HTTP sessionをクローズしました"
                )
            except Exception as e:
                self.logger.error(
                    f"{self.__class__.__name__}: sessionクローズ中にエラー: {str(e)}"
                )
