"""
ステップ5: Winticketのレース結果取得API
"""

import logging

# import json # F401 未使用のため削除
import re
import time

import requests


class WinticketStep5API:
    """
    ステップ5: Winticketのレース結果を取得するAPI
    """

    # API基本URL
    BASE_URL = "https://api.winticket.jp/v1/keirin"

    # APIエンドポイント
    ENDPOINTS = {"race_result": "/races/{race_id}/result"}

    def __init__(self, rate_limiter=None, logger=None, session=None):
        """
        初期化

        Args:
            rate_limiter: APIレート制限用のインスタンス
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
            session: 既存のrequestsセッション（省略時は新規作成）
        """
        self.logger = logger or logging.getLogger(__name__)
        self.rate_limiter = rate_limiter

        # セッション初期化（引数で指定されていればそれを使用、なければ新規作成）
        self.session = session or requests.Session()

        # ユーザーエージェント設定
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "https://keirin.winticket.jp",
                "Referer": "https://keirin.winticket.jp/",
            }
        )

        # リクエスト間隔（秒）- rate_limiterがない場合のデフォルト値
        self.request_interval = 1.0

        # 最後のリクエスト時刻
        self.last_request_time = 0

    def _throttle_request(self):
        """APIリクエストのスロットリング（間隔調整）"""
        if self.rate_limiter:
            self.rate_limiter.wait()
        else:
            current_time = time.time()
            elapsed = current_time - self.last_request_time

            # 前回のリクエストからinterval秒以上経過していない場合は待機
            if elapsed < self.request_interval:
                wait_time = self.request_interval - elapsed
                self.logger.debug(
                    f"APIリクエスト間隔調整のため {wait_time:.2f}秒 待機します"
                )
                time.sleep(wait_time)

            # 最終リクエスト時刻を更新
            self.last_request_time = time.time()

    def _make_api_request(self, endpoint, params=None, retry_count=3):
        """
        APIリクエストを実行

        Args:
            endpoint (str): APIのエンドポイント
            params (dict, optional): クエリパラメータ
            retry_count (int, optional): リトライ回数

        Returns:
            dict or None: APIレスポンス（JSONをパースしたもの）、エラー時はNone
        """
        url = f"{self.BASE_URL}{endpoint}"

        for attempt in range(retry_count):
            try:
                # リクエスト間隔調整
                self._throttle_request()

                # リクエスト開始ログ
                self.logger.debug(f"APIリクエスト実行: {url} (params: {params})")
                start_time = time.time()

                # リクエスト実行
                response = self.session.get(url, params=params, timeout=30)

                # リクエスト完了ログ
                elapsed = time.time() - start_time
                self.logger.debug(
                    f"APIレスポンス受信: {url} (ステータスコード: {response.status_code}, 処理時間: {elapsed:.2f}秒)"
                )

                # ステータスコードチェック
                if response.status_code == 200:
                    # JSONレスポンスのパース
                    try:
                        return response.json()
                    except Exception as json_err:
                        self.logger.error(f"JSONパースエラー: {url} - {str(json_err)}")
                        self.logger.debug(f"レスポンス内容: {response.text[:500]}...")
                        return None
                else:
                    self.logger.warning(
                        f"APIリクエストエラー: {url} (ステータスコード: {response.status_code})"
                    )

                    # レスポンスの内容をログに出力
                    try:
                        error_info = response.json()
                        self.logger.warning(f"エラーレスポンス: {error_info}")
                    except Exception:
                        self.logger.warning(f"エラーレスポンス: {response.text[:500]}")

                    # リトライ判断
                    if response.status_code == 429:  # レート制限
                        retry_after = int(response.headers.get("Retry-After", 60))
                        self.logger.warning(
                            f"レート制限エラー。{retry_after}秒後にリトライします (試行: {attempt+1}/{retry_count})"
                        )
                        time.sleep(retry_after)
                    elif response.status_code >= 500:  # サーバーエラー
                        retry_wait = (attempt + 1) * 3  # 指数バックオフ
                        self.logger.warning(
                            f"サーバーエラー。{retry_wait}秒後にリトライします (試行: {attempt+1}/{retry_count})"
                        )
                        time.sleep(retry_wait)
                    else:  # その他のエラー
                        self.logger.error(
                            f"APIリクエストが失敗しました: {url} (ステータスコード: {response.status_code})"
                        )
                        return None

            except requests.RequestException as e:
                self.logger.error(f"APIリクエスト例外発生: {url} - {str(e)}")
                retry_wait = (attempt + 1) * 3  # 指数バックオフ
                self.logger.warning(
                    f"通信エラー。{retry_wait}秒後にリトライします (試行: {attempt+1}/{retry_count})"
                )
                time.sleep(retry_wait)

        # すべてのリトライが失敗
        self.logger.error(f"すべてのリトライが失敗しました: {url}")
        return None

    def get_race_result(self, race_id):
        """
        レースの結果情報を取得

        Args:
            race_id (str): レースID

        Returns:
            dict or None: 結果情報、取得失敗時はNone
        """
        # 結果APIエンドポイント
        endpoint = self.ENDPOINTS["race_result"].format(race_id=race_id)

        # APIリクエスト
        self.logger.info(f"レース {race_id} の結果情報を取得します")

        try:
            result_data = self._make_api_request(endpoint)

            if not result_data:
                self.logger.warning(f"レース {race_id} の結果情報取得に失敗しました")
                return None

            # 結果データの検証
            if "order" not in result_data or not result_data["order"]:
                self.logger.warning(f"レース {race_id} の結果データが不完全です")
                return None

            self.logger.info(
                f"レース {race_id} の結果情報を取得しました（着順数: {len(result_data.get('order', []))}）"
            )
            return result_data

        except Exception as e:
            self.logger.error(
                f"レース結果情報の取得中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def extract_race_params_from_id(self, race_id):
        """
        レースIDからカップID、スケジュールインデックス、レース番号を抽出

        Args:
            race_id (str): レースID（形式: cup_id_schedule_index_race_number）

        Returns:
            tuple: (cup_id, schedule_index, race_number) または None（抽出失敗時）
        """
        try:
            # パターンに一致するか確認
            pattern = r"^(.+)_(\d+)_(\d+)$"
            match = re.match(pattern, race_id)

            if match:
                cup_id = match.group(1)
                schedule_index = int(match.group(2))
                race_number = int(match.group(3))
                return cup_id, schedule_index, race_number
            else:
                self.logger.error(
                    f"レースID '{race_id}' からパラメータを抽出できませんでした"
                )
                return None

        except Exception as e:
            self.logger.error(f"レースIDからのパラメータ抽出中にエラー: {str(e)}")
            return None

    def get_completed_races(self, date_str):
        """
        指定日の完了済みレース一覧を取得（オプション機能）

        Args:
            date_str (str): 日付文字列（YYYYMMDD形式）

        Returns:
            list or None: 完了済みレースのリスト、取得失敗時はNone
        """
        # 日付フォーマットのチェック
        if not re.match(r"^\d{8}$", date_str):
            self.logger.error(
                f"日付のフォーマットが不正です: {date_str} (YYYYMMDD形式である必要があります)"
            )
            return None

        # 日付ごとのレース一覧を取得するエンドポイント
        endpoint = f"/races/date/{date_str}"

        # APIリクエスト
        self.logger.info(f"{date_str}の完了済みレース一覧を取得します")

        try:
            races_data = self._make_api_request(endpoint)

            if not races_data or "races" not in races_data:
                self.logger.warning(f"{date_str}のレース一覧取得に失敗しました")
                return None

            # 完了済みレースのみをフィルタリング
            completed_races = []
            for race in races_data["races"]:
                if race.get("status") == "decided":
                    completed_races.append(race)

            self.logger.info(f"{date_str}の完了済みレース: {len(completed_races)}件")
            return completed_races

        except Exception as e:
            self.logger.error(
                f"完了済みレース一覧の取得中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return None

    def cleanup(self):
        """セッションのクリーンアップ処理"""
        if hasattr(self, "session") and self.session:
            try:
                self.session.close()
                self.logger.debug("セッションをクローズしました")
            except Exception as e:
                self.logger.error(f"セッションクローズ中にエラー: {str(e)}")
