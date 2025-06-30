"""
Winticketサイトの情報を取得するためのAPIクライアント
"""

import json
import logging
import time
from datetime import datetime

import requests

from .api_rate_limiter import ApiBackoff, APIRateLimiter  # noqa: F401

# バージョン情報（setup.pyや他の方法で動的に設定することも検討）
__version__ = "0.1.0"

# ユーザーエージェントの基本形
USER_AGENT_BASE = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
)
USER_AGENT = f"{USER_AGENT_BASE} KeirinApp/{__version__}"


class WinticketAPI:
    """
    WinticketサイトからのAPI呼び出しを行うクラス
    """

    # API基本URL
    BASE_URL = "https://api.winticket.jp/v1/keirin"

    # APIエンドポイント
    ENDPOINTS = {
        "cups": "/cups",
        "cup_detail": "/cups/{cup_id}",
        "race": "/cups/{cup_id}/schedules/{index}/races/{race_number}",
        "odds": "/cups/{cup_id}/schedules/{index}/races/{race_number}/odds",
    }

    def __init__(self, logger=None):
        """
        初期化処理

        Args:
            logger (logging.Logger, optional): ロガーインスタンス
        """
        # ロガーの設定
        self.logger = logger or logging.getLogger(__name__)

        # セッション初期化
        self.session = requests.Session()

        # ユーザーエージェント設定
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "Origin": "https://www.winticket.jp",
            }
        )

        # リクエスト間隔（秒）
        self.request_interval = 1.0

        # 最後のリクエスト時刻
        self.last_request_time = 0

        # 初期化済みフラグ
        self._initialized = True

        self.logger.debug("WinticketAPIクライアントを初期化しました")

    def _throttle_request(self):
        """APIリクエストのスロットリング（間隔調整）"""
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

    def _make_api_request(
        self, endpoint, params=None, data=None, method="GET", retry_count=3
    ):
        """
        APIリクエストを実行

        Args:
            endpoint (str): APIエンドポイント
            params (dict, optional): クエリパラメータ
            data (dict, optional): POSTリクエストのボディデータ
            method (str, optional): HTTPメソッド (GET, POSTなど)
            retry_count (int, optional): リトライ回数

        Returns:
            dict or None: APIレスポンス（JSONをパースしたもの）、エラー時はNone
        """
        url = f"{self.BASE_URL}{endpoint}"
        headers_with_auth = self.session.headers.copy()
        # ここで必要に応じて認証トークンなどをヘッダーに追加するロジックを実装
        # 例: if self.auth_token: headers_with_auth['Authorization'] = f'Bearer {self.auth_token}'

        for attempt in range(retry_count):
            try:
                # リクエスト間隔調整
                self._throttle_request()

                # リクエスト前にログ出力
                # 認証ヘッダーを除いたヘッダー情報（ログ出力用）
                headers_for_log = {
                    k: v
                    for k, v in headers_with_auth.items()
                    if k.lower() != "authorization"
                }
                # ボディ部分を事前に文字列化
                body_for_log = json.dumps(data) if data else "{}"
                debug_message = (
                    f"API Request: {method.upper()} {url} | Params: {params} | "
                    f"Headers: {headers_for_log} | Body: {body_for_log}"
                )
                self.logger.debug(debug_message)

                # リクエスト実行
                response = self.session.request(
                    method, url, params=params, json=data, timeout=30
                )

                # リクエスト完了ログ
                elapsed = time.time() - self.last_request_time
                self.logger.debug(
                    f"APIレスポンス受信: {url} (ステータスコード: {response.status_code}, 処理時間: {elapsed:.2f}秒)"
                )

                # レスポンスボディをログ出力 (デバッグ用に常に出力、または status code で分岐)
                # response.text は負荷が高い場合があるので注意。必要なら DEBUG レベルにする。
                try:
                    response_body_preview = response.text[
                        :1000
                    ]  # 長すぎる場合は切り詰める
                    self.logger.debug(
                        f"APIレスポンスボディ (プレビュー): {response_body_preview}"
                    )
                except Exception as log_err:
                    self.logger.warning(
                        f"レスポンスボディのロギング中にエラー: {log_err}"
                    )

                # ステータスコードチェック
                if response.status_code == 200:
                    # 成功時でも中身を確認するために DEBUG ログ
                    try:
                        json_data = response.json()
                        json_str = json.dumps(json_data, ensure_ascii=False, indent=2)
                        self.logger.debug(
                            f"API成功レスポンス (JSON): {json_str}"
                        )  # 詳細なJSONログを追加
                        return json_data
                    except json.JSONDecodeError as json_err:
                        self.logger.error(
                            f"API成功レスポンスのJSONデコードに失敗: {json_err}. Body: {response.text[:500]}"
                        )
                        return None  # またはエラーを再raiseする
                else:
                    # 失敗時はレスポンスボディ全体をWARNINGでログ出力
                    self.logger.warning(
                        f"APIリクエストエラー: {url} (ステータスコード: {response.status_code})"
                    )
                    self.logger.warning(f"エラーレスポンスボディ: {response.text}")

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
                    else:  # その他のクライアントエラー
                        return None  # リトライせず None を返す

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

    def cleanup(self):
        """セッションのクリーンアップ処理"""
        if hasattr(self, "session") and self.session:
            try:
                self.session.close()
                self.logger.debug("セッションをクローズしました")
            except Exception as e:
                self.logger.error(f"セッションクローズ中にエラー: {str(e)}")

    def get_monthly_cups(self, date_str):
        """
        指定月の開催情報を取得

        Args:
            date_str (str): 日付文字列（YYYYMMDD形式）- 月の初日を指定

        Returns:
            dict: 開催情報（カップ情報、日程など）のディクショナリ
        """
        # 日付をdatetimeオブジェクトに変換
        try:
            target_date = datetime.strptime(date_str, "%Y%m%d")
            # 日付が1日でない場合は警告
            if target_date.day != 1:
                self.logger.warning(
                    f"date_strは月の初日を指定することを推奨: {date_str}"
                )
        except ValueError:
            self.logger.error(f"無効な日付形式です: {date_str}")
            return None

        # APIリクエスト
        self.logger.info(f"{target_date.strftime('%Y年%m月')}の開催情報を取得します")
        endpoint = self.ENDPOINTS["cups"]
        params = {"date": date_str, "fields": "month", "pfm": "web"}

        try:
            response = self._make_api_request(endpoint, params)
            if not response:
                self.logger.error(f"開催情報の取得に失敗しました: {date_str}")
                return None

            self.logger.info(
                f"{len(response.get('month', {}).get('cups', []))}件の開催情報を取得しました"
            )
            return response

        except Exception as e:
            self.logger.error(
                f"開催情報の取得中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def get_cup_detail(self, cup_id):
        """
        特定の開催の詳細情報を取得

        Args:
            cup_id (str): 開催ID

        Returns:
            dict: 開催詳細情報のディクショナリ
        """
        # APIリクエスト
        self.logger.info(f"開催ID {cup_id} の詳細情報を取得します")
        endpoint = self.ENDPOINTS["cup_detail"].format(cup_id=cup_id)

        # クエリパラメータ
        params = {"fields": "cup,schedules,races", "pfm": "web"}

        try:
            response = self._make_api_request(endpoint, params)
            if not response:
                self.logger.error(f"開催詳細情報の取得に失敗しました: {cup_id}")
                return None

            self.logger.info(f"開催 {cup_id} の詳細情報を取得しました")
            return response

        except Exception as e:
            self.logger.error(
                f"開催詳細情報の取得中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def get_race_info(self, cup_id, index, race_number):
        """
        レース詳細情報（選手/出走表/成績/ライン）の取得
        ※ メソッド名は get_race_info のままですが、取得フィールドを変更

        Args:
            cup_id (str): 開催ID
            index (int): スケジュールインデックス (day)
            race_number (int): レース番号

        Returns:
            dict: APIレスポンス全体のディクショナリ or None
        """
        endpoint = self.ENDPOINTS["race"].format(
            cup_id=cup_id, index=index, race_number=race_number
        )
        self.logger.info(f"構築されたエンドポイントパス: {endpoint}")

        # クエリパラメータ: 取得フィールドを変更
        params = {"fields": "players,entries,records,linePrediction", "pfm": "web"}

        self.logger.info(
            f"レース詳細情報（選手/出走表/成績/ライン）を取得します: 開催ID {cup_id}, 日 {index}, R {race_number}"
        )

        try:
            response = self._make_api_request(endpoint, params)

            if response is None:
                self.logger.error(
                    f"レース詳細情報（選手/出走表/成績/ライン）の取得に失敗 (APIエラー): {cup_id}, {index}, {race_number}"
                )
                return None

            # レスポンスに必要なキーが最低一つ含まれているかチェック
            required_keys = [
                "players",
                "entries",
                "records",
            ]  # linePrediction はオプショナルかもしれない
            if not any(key in response for key in required_keys):
                error_message = (
                    f"レース詳細情報（選手/出走表/成績/ライン）のレスポンスに必要なキー ({required_keys}) "
                    f"が含まれていません: {cup_id}, {index}, {race_number}. Response: {response}"
                )
                self.logger.error(error_message)
                return None

            self.logger.info(
                f"レース詳細情報（選手/出走表/成績/ライン）を取得しました: {cup_id}, {index}, {race_number}"
            )
            return response

        except Exception as e:
            self.logger.error(
                f"レース詳細情報（選手/出走表/成績/ライン）の取得中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return None

    def get_race_entry(self, cup_id, index, race_number):
        """
        レース出走表情報の取得

        Args:
            cup_id (str): 開催ID
            index (int): スケジュールインデックス
            race_number (int): レース番号

        Returns:
            dict: 出走表情報のディクショナリ
        """
        # APIリクエスト
        endpoint = self.ENDPOINTS["race"].format(
            cup_id=cup_id, index=index, race_number=race_number
        )

        # クエリパラメータ
        params = {"fields": "race,entries,players,records,linePrediction", "pfm": "web"}

        self.logger.info(
            f"レース出走表情報を取得します: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
        )

        try:
            response = self._make_api_request(endpoint, params)
            if not response:
                self.logger.error(
                    f"レース出走表情報の取得に失敗しました: {cup_id}, {index}, {race_number}"
                )
                return None

            if "entries" not in response:
                self.logger.error(
                    f"レース出走表情報のレスポンスにentriesデータがありません: {cup_id}, {index}, {race_number}"
                )
                return None

            # 基本データを格納
            entry_info = {
                "race": response.get("race", {}),
                "entries": response.get("entries", []),
            }

            # 選手情報がある場合は追加
            if "players" in response and isinstance(response["players"], list):
                entry_info["players"] = response["players"]
                self.logger.debug(
                    f"選手情報を取得しました: {len(response['players'])}件"
                )
            else:
                entry_info["players"] = []
                self.logger.debug(
                    "選手情報がレスポンスに含まれていないか、不正な形式です"
                )

            # レコード情報がある場合は追加
            if "records" in response and isinstance(response["records"], list):
                entry_info["records"] = response["records"]
                self.logger.debug(
                    f"レコード情報を取得しました: {len(response['records'])}件"
                )
            else:
                entry_info["records"] = []
                self.logger.debug(
                    "レコード情報がレスポンスに含まれていないか、不正な形式です"
                )

            # ライン予測情報がある場合は追加
            if "linePrediction" in response and isinstance(
                response["linePrediction"], dict
            ):
                line_prediction = response["linePrediction"]
                if "lineType" in line_prediction and "lines" in line_prediction:
                    entry_info["linePrediction"] = line_prediction
                    lines_count = len(line_prediction.get("lines", []))
                    self.logger.debug(
                        f"ライン予測情報を取得しました: {lines_count}件のライン"
                    )
                else:
                    entry_info["linePrediction"] = {"lineType": "", "lines": []}
                    self.logger.debug("ライン予測情報の形式が不正です")
            else:
                entry_info["linePrediction"] = {"lineType": "", "lines": []}
                self.logger.debug(
                    "ライン予測情報がレスポンスに含まれていないか、不正な形式です"
                )

            entry_count = len(entry_info["entries"])
            self.logger.info(
                f"レース出走表情報を取得しました: {cup_id}, {index}, {race_number}, 選手数: {entry_count}件"
            )

            return entry_info

        except Exception as e:
            self.logger.error(
                f"レース出走表情報の取得中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return None

    def get_race_odds(self, cup_id, index, race_number):
        """
        レースオッズ情報の取得

        Args:
            cup_id (str): 開催ID
            index (int): スケジュールインデックス
            race_number (int): レース番号

        Returns:
            dict: オッズ情報のディクショナリ
        """
        # APIリクエスト
        endpoint = self.ENDPOINTS["odds"].format(
            cup_id=cup_id, index=index, race_number=race_number
        )

        # クエリパラメータ
        params = {"pfm": "web"}

        self.logger.info(
            f"レースオッズ情報を取得します: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
        )

        try:
            response = self._make_api_request(endpoint, params)
            if not response:
                self.logger.error(
                    f"レースオッズ情報の取得に失敗しました: {cup_id}, {index}, {race_number}"
                )
                return None

            # オッズの種類をチェック
            odds_types = [
                "win",
                "place",
                "bracketQuinella",
                "bracketExacta",
                "quinella",
                "exacta",
                "trio",
                "trifecta",
            ]

            odds_info = {"odds": {}}

            for odds_type in odds_types:
                if odds_type in response:
                    odds_info["odds"][odds_type] = response[odds_type]

            fetched_odds_types = ", ".join(odds_info["odds"].keys())
            self.logger.info(
                f"レースオッズ情報を取得しました: {cup_id}, {index}, {race_number}, 種類: {fetched_odds_types}"
            )
            return odds_info

        except Exception as e:
            self.logger.error(
                f"レースオッズ情報の取得中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return None

    def get_race_result(self, cup_id, index, race_number):
        """
        レース結果情報の取得

        Args:
            cup_id (str): 開催ID
            index (int): スケジュールインデックス
            race_number (int): レース番号

        Returns:
            dict: 結果情報のディクショナリ
        """
        # APIリクエスト
        endpoint = self.ENDPOINTS["race"].format(
            cup_id=cup_id, index=index, race_number=race_number
        )

        # クエリパラメータ
        params = {"fields": "race,entries,players,results", "pfm": "web"}

        self.logger.info(
            f"レース結果情報を取得します: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
        )

        try:
            response = self._make_api_request(endpoint, params)
            if not response or "race" not in response:
                self.logger.error(
                    f"レース結果情報の取得に失敗しました: {cup_id}, {index}, {race_number}"
                )
                return None

            # レースのステータスをチェック（終了していない場合はNoneを返す）
            race = response.get("race", {})
            if race.get("status") != 3:  # ステータス3はレース終了
                self.logger.warning(
                    f"レース {cup_id}, {index}, {race_number} はまだ終了していません（ステータス: {race.get('status')}）"
                )
                return None

            result_info = {}

            # 結果情報がある場合
            if "results" in response:
                result_info["results"] = response["results"]
            elif "entries" in response:
                # 結果情報がなくてもエントリー情報の中に着順がある場合
                entries_with_ranks = []
                for entry in response["entries"]:
                    if "rank" in entry and entry["rank"] > 0:
                        entry_result = {
                            "playerId": entry.get("playerId", ""),
                            "playerName": entry.get("playerName", ""),
                            "rank": entry.get("rank", 0),
                            "time": entry.get("time", ""),
                            "winOdds": entry.get("winOdds", 0),
                            "placeOdds": entry.get("placeOdds", 0),
                        }
                        entries_with_ranks.append(entry_result)

                if entries_with_ranks:
                    result_info["results"] = sorted(
                        entries_with_ranks, key=lambda x: x["rank"]
                    )

            if not result_info.get("results"):
                self.logger.warning(
                    f"レース {cup_id}, {index}, {race_number} の結果情報が見つかりません"
                )
                return None

            race_key_info = f"レース結果取得: {cup_id}, {index}, R{race_number}"
            results_count = len(result_info.get("results", []))
            self.logger.info(f"{race_key_info}, 結果数: {results_count}")
            return result_info

        except Exception as e:
            self.logger.error(
                f"レース結果情報の取得中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def get_cups_info(self, date_str):
        """
        指定月の開催情報を取得

        Args:
            date_str (str): 日付文字列（YYYYMMDD形式）- 月の初日を指定

        Returns:
            dict: 開催情報（カップ情報、日程など）のディクショナリ
        """
        return self.get_monthly_cups(date_str)

    def get_event_list(self, start_date, end_date):
        """
        指定期間のイベント(開催)一覧を取得

        Args:
            start_date (date): 開始日
            end_date (date): 終了日

        Returns:
            list: イベント情報のリスト
        """
        self.logger.info(
            f"期間 {start_date} から {end_date} のイベント一覧を取得します"
        )

        # 期間をカバーする月ごとのデータを取得
        events = []

        # 開始年月を取得
        current_year = start_date.year
        current_month = start_date.month

        # 終了年月を取得
        end_year = end_date.year
        end_month = end_date.month

        while (current_year < end_year) or (
            current_year == end_year and current_month <= end_month
        ):
            # 月初日のYYYYMMDD形式を取得
            month_str = f"{current_year}{current_month:02d}01"

            try:
                # 月間データを取得
                monthly_data = self.get_monthly_cups(month_str)

                if (
                    monthly_data
                    and "month" in monthly_data
                    and "cups" in monthly_data["month"]
                ):
                    cups = monthly_data["month"]["cups"]

                    # 期間内の開催のみをフィルタリング
                    for cup in cups:
                        try:
                            # 開始日と終了日のチェック
                            start_date_str = cup.get("startDate")
                            end_date_str = cup.get("endDate")

                            if not start_date_str or not end_date_str:
                                continue

                            # 日付形式を確認して変換
                            cup_start = None
                            cup_end = None

                            if "-" in start_date_str:
                                cup_start = datetime.strptime(
                                    start_date_str, "%Y-%m-%d"
                                ).date()
                            else:
                                cup_start = datetime.strptime(
                                    start_date_str, "%Y%m%d"
                                ).date()

                            if "-" in end_date_str:
                                cup_end = datetime.strptime(
                                    end_date_str, "%Y-%m-%d"
                                ).date()
                            else:
                                cup_end = datetime.strptime(
                                    end_date_str, "%Y%m%d"
                                ).date()

                            # 期間内かどうかをチェック
                            if cup_end >= start_date and cup_start <= end_date:
                                # APIデータをイベントリスト用に整形
                                event_data = {
                                    "cup_id": cup.get("id"),
                                    "cup_name": cup.get("name"),
                                    "venue_name": cup.get("venueName", ""),
                                    "region_name": cup.get("regionName", ""),
                                    "start_date": start_date_str,
                                    "end_date": end_date_str,
                                    "days": cup.get("days", 0),
                                    "status": cup.get("status", "active"),
                                }

                                # 会場IDと地域IDがある場合は追加
                                if "venueId" in cup:
                                    event_data["venue_id"] = cup.get("venueId")

                                if "regionId" in cup:
                                    event_data["region_id"] = cup.get("regionId")

                                events.append(event_data)
                        except Exception as e:
                            self.logger.warning(
                                f"カップデータの処理中にエラー: {str(e)}"
                            )
                            continue
            except Exception as e:
                self.logger.error(f"{month_str}の月間データ取得中にエラー: {str(e)}")

            # 次の月へ
            if current_month == 12:
                current_year += 1
                current_month = 1
            else:
                current_month += 1

        self.logger.info(f"期間内のイベント {len(events)}件を取得しました")
        return events

    def get_race_entries(self, cup_id, index, race_number):
        """
        レースの出走選手情報の取得

        Args:
            cup_id (str): 開催ID
            index (int): スケジュールインデックス
            race_number (int): レース番号

        Returns:
            dict: 出走選手情報のディクショナリ
        """
        # APIリクエスト
        endpoint = self.ENDPOINTS["race"].format(
            cup_id=cup_id, index=index, race_number=race_number
        )

        # クエリパラメータ - エントリー情報を取得
        params = {"fields": "entries,players", "pfm": "web"}

        self.logger.info(
            f"レース出走選手情報を取得します: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
        )

        try:
            response = self._make_api_request(endpoint, params)
            if not response:
                self.logger.warning(
                    f"レース出走選手情報の取得に失敗しました: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
                )
                return None

            # エントリー情報と選手情報を抽出
            entry_data = {
                "entries": response.get("entries", []),
                "players": response.get("players", []),
            }

            self.logger.debug(
                f"レース出走選手情報を取得しました: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
            )
            return entry_data
        except Exception as e:
            self.logger.error(
                f"レース出走選手情報の取得中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return None

    def get_odds_data(self, cup_id, index, race_number):
        """
        オッズ情報の取得

        Args:
            cup_id (str): 開催ID
            index (int): スケジュールインデックス
            race_number (int): レース番号

        Returns:
            dict: オッズ情報のディクショナリ
        """
        self.logger.info(
            f"レースのオッズデータを取得します: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
        )

        # APIリクエスト
        endpoint = self.ENDPOINTS["odds"].format(
            cup_id=cup_id, index=index, race_number=race_number
        )

        # クエリパラメータ
        params = {"pfm": "web"}

        try:
            response = self._make_api_request(endpoint, params)
            if not response:
                self.logger.warning(
                    f"オッズデータの取得に失敗しました: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
                )
                return None

            self.logger.debug(
                f"オッズデータを取得しました: 開催ID {cup_id}, スケジュール {index}, レース番号 {race_number}"
            )
            return response
        except Exception as e:
            self.logger.error(
                f"オッズデータの取得中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def get_races_for_date(self, date_str):
        """
        指定日のレース情報リストを取得

        Args:
            date_str (str): 日付文字列（YYYYMMDD形式）

        Returns:
            list: レース情報のリスト（形式: cup_id, index, race_number のタプル）
        """
        # 日付をdatetimeオブジェクトに変換
        try:
            target_date = datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            self.logger.error(f"無効な日付形式です: {date_str}")
            return []

        # 月初の日付を取得 (1日)
        first_day = target_date.replace(day=1)
        first_day_str = first_day.strftime("%Y%m%d")

        # カップ情報を取得
        self.logger.info(
            f"{target_date.strftime('%Y年%m月%d日')}のレース情報を取得します"
        )
        cups_info = self.get_monthly_cups(first_day_str)

        if (
            not cups_info
            or "month" not in cups_info
            or "cups" not in cups_info["month"]
            or not cups_info["month"]["cups"]
        ):
            self.logger.warning(
                f"カップ情報の取得に失敗したか、カップが見つかりませんでした: {date_str}"
            )
            return []

        race_info = []

        # 各カップについて処理
        for cup in cups_info["month"]["cups"]:
            cup_id = cup.get("id")
            if not cup_id:
                continue

            # カップ詳細を取得
            cup_detail = self.get_cup_detail(cup_id)
            if (
                not cup_detail
                or "schedules" not in cup_detail
                or not cup_detail["schedules"]
            ):
                continue

            # 各スケジュールについて処理
            for schedule in cup_detail["schedules"]:
                schedule_date_str = schedule.get("date")
                if not schedule_date_str:
                    continue

                # 日付が一致するか確認
                try:
                    schedule_date = datetime.strptime(
                        schedule_date_str, "%Y-%m-%d"
                    ).date()
                    if schedule_date != target_date:
                        continue

                    # スケジュールインデックスを取得
                    index = schedule.get("index")

                    # 各レースについて処理
                    if "races" in cup_detail and cup_detail["races"]:
                        for race in cup_detail["races"]:
                            if race.get("scheduleId") == schedule.get("id"):
                                race_number = race.get("number")
                                # タプル形式でレース情報を保存
                                race_info.append((cup_id, index, race_number))
                except Exception as e:
                    self.logger.error(
                        f"スケジュール日付の変換中にエラー: {schedule_date_str} - {str(e)}"
                    )
                    continue

        self.logger.info(
            f"{target_date.strftime('%Y年%m月%d日')}の{len(race_info)}件のレース情報を取得しました"
        )
        return race_info
