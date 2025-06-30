"""
YenjoyサイトからAPI経由でレースの周回情報などを取得するクライアント
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional, Union  # noqa: F401

# import json # 未使用のため削除
import requests
from bs4 import BeautifulSoup

# ユーザーエージェントの基本形
USER_AGENT_BASE = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/91.0.4472.124 Safari/537.36"
)
USER_AGENT = f"{USER_AGENT_BASE} YenjoyApp/0.1.0"


class YenjoyAPI:
    """
    YenjoyサイトからのAPI呼び出しを行うクラス
    """

    # API基本URL
    BASE_URL = "https://www.yen-joy.net"

    # APIエンドポイント
    ENDPOINTS = {
        "race_detail": "/race-detail/",
        "position_api": "/race-positions/api/{race_id}/positions.json",
    }

    def __init__(self, winticket_api=None, logger=None, rate_limit_wait=None):
        """
        初期化処理

        Args:
            winticket_api: WinticketAPIのインスタンス（IDマッピングに使用、現在は未使用かも）
            logger (logging.Logger, optional): ロガーインスタンス
            rate_limit_wait (float, optional): リクエスト間隔（秒）
        """
        # ロガーの設定
        self.logger = logger or logging.getLogger(__name__)

        # Winticket APIの参照
        self.winticket_api = winticket_api

        # セッション初期化
        self.session = requests.Session()

        # ユーザーエージェント設定
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "https://yenjoy.keirin.jp",
                "Referer": "https://yenjoy.keirin.jp/",
            }
        )

        # リクエスト間隔（秒）- config.iniの設定値を使用、未指定時は1.0秒
        self.request_interval = rate_limit_wait if rate_limit_wait is not None else 1.0

        # 最後のリクエスト時刻
        self.last_request_time = 0

        self.logger.debug("YenjoyAPIクライアントを初期化しました")

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

    def _make_api_request(self, url, params=None, retry_count=3):
        """
        APIリクエストを実行

        Args:
            url (str): APIのURL
            params (dict, optional): クエリパラメータ
            retry_count (int, optional): リトライ回数

        Returns:
            dict or None: APIレスポンス（JSONをパースしたもの）、エラー時はNone
        """
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

    def get_race_position_data(self, race_id, bracket_to_player=None):
        """
        レースの位置情報データを取得

        Args:
            race_id (str): Winticketのレース ID
            bracket_to_player (dict, optional): 車番と選手IDのマッピング辞書

        Returns:
            dict or None: レースの位置情報データ、取得失敗時はNone
        """
        # Yenjoy の race_id に変換
        yenjoy_race_id = self._convert_to_yenjoy_race_id(race_id)
        if not yenjoy_race_id:
            self.logger.error(f"Yenjoyレース ID の変換に失敗しました: {race_id}")
            return None

        # 位置情報APIエンドポイント
        endpoint_path = self.ENDPOINTS["position_api"].format(race_id=yenjoy_race_id)
        position_url = f"{self.BASE_URL}{endpoint_path}"

        # APIリクエスト
        self.logger.info(
            f"レース {race_id} (Yenjoy ID: {yenjoy_race_id}) の位置情報を取得します"
        )
        try:
            position_data = self._make_api_request(position_url)

            if not position_data:
                self.logger.warning(f"レース {race_id} の位置情報取得に失敗しました")
                return None

            # 位置情報データの検証
            if not self._validate_position_data(position_data):
                self.logger.warning(
                    f"レース {race_id} の位置情報フォーマットが不正です"
                )
                return None

            # 詳細な位置情報データを取得
            position_details = self._parse_position_details(
                position_data, bracket_to_player
            )
            if not position_details:
                self.logger.warning(
                    f"レース {race_id} の詳細位置情報の解析に失敗しました"
                )
                return None

            self.logger.info(f"レース {race_id} の位置情報を取得しました")
            return position_details

        except Exception as e:
            self.logger.error(
                f"レース位置情報の取得中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def _parse_position_details(self, position_data, bracket_to_player=None):
        """
        位置情報データから詳細なポジションデータを解析

        Args:
            position_data (dict): 位置情報データ
            bracket_to_player (dict, optional): 車番と選手IDのマッピング辞書

        Returns:
            list: ポイントごとの選手位置データのリスト
        """
        try:
            # 結果データを格納するリスト
            position_details = []

            # racing データからセクションごとの情報を取得
            racing_data = position_data.get("racing", [])

            # レーサー情報を取得
            racers = {}
            for racer in position_data.get("racers", []):
                racer_id = str(racer.get("id", ""))
                frame_number = racer.get("frameNumber", 0)
                name = racer.get("name", "")
                racers[racer_id] = {"frame_number": frame_number, "name": name}

            # 各周回(セクション)のデータを処理
            for lap_data in racing_data:
                lap_number = lap_data.get("lap", 0)
                lap_count = lap_data.get("lapCount", 0.0)
                section_name = f"第{lap_number}周回"

                # 各選手の位置データを処理
                for racer_id, position in lap_data.get("positions", {}).items():
                    if racer_id in racers:
                        racer = racers[racer_id]
                        bracket_number = racer["frame_number"]
                        racer_name = racer["name"]

                        # x, y 座標を取得
                        x_val = position.get("x", 0)
                        y_val = position.get("y", 0)

                        # 順位情報
                        order = position.get("order", 0)

                        # 車番でフィルタリング（指定がある場合）
                        if (
                            bracket_to_player
                            and str(bracket_number) not in bracket_to_player.keys()
                        ):
                            continue

                        # 詳細位置データを追加
                        position_detail = {
                            "bracket": bracket_number,
                            "player_name": racer_name,
                            "lap": lap_number,
                            "lap_count": lap_count,
                            "x": x_val,
                            "y": y_val,
                            "position": order,
                            "section": section_name,
                        }

                        position_details.append(position_detail)

            return position_details

        except Exception as e:
            self.logger.error(
                f"位置情報データの解析中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def _convert_to_yenjoy_race_id(self, winticket_race_id):
        """
        WinticketのレースIDをYenjoyのレースIDに変換

        Args:
            winticket_race_id (str): WinticketのレースID

        Returns:
            str or None: YenjoyのレースID
        """
        try:
            # Winticketのレース情報を取得
            if not self.winticket_api:
                self.logger.error("Winticket APIが設定されていません")
                return None

            # レースIDを分解
            parts = winticket_race_id.split("_")
            if len(parts) != 3:
                self.logger.error(
                    f"無効なWinticketレースID形式です: {winticket_race_id}"
                )
                return None

            cup_id = parts[0]
            schedule_idx = int(parts[1])
            race_number = int(parts[2])

            # カップ詳細を取得
            cup_detail = self.winticket_api.get_cup_detail(cup_id)
            if (
                not cup_detail
                or "schedules" not in cup_detail
                or len(cup_detail["schedules"]) <= schedule_idx
            ):
                self.logger.error(f"カップ詳細の取得に失敗しました: {cup_id}")
                return None

            # スケジュール情報を取得
            schedule = cup_detail["schedules"][schedule_idx]

            # レース情報を取得
            race_info = None
            if "races" in schedule:
                for race in schedule["races"]:
                    if race.get("race_number") == race_number:
                        race_info = race
                        break

            if not race_info:
                self.logger.error(
                    f"レース情報が見つかりませんでした: {winticket_race_id}"
                )
                return None

            # Yenjoy IDを取得
            yenjoy_id = race_info.get("yenjoy_id")
            if not yenjoy_id:
                self.logger.error(
                    f"YenjoyIDが見つかりませんでした: {winticket_race_id}"
                )
                return None

            self.logger.debug(
                f"WinticketレースID {winticket_race_id} をYenjoyレースID {yenjoy_id} に変換しました"
            )
            return yenjoy_id

        except Exception as e:
            self.logger.error(
                f"YenjoyレースIDへの変換中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return None

    def _validate_position_data(self, position_data):
        """
        位置情報データの検証

        Args:
            position_data (dict): 位置情報データ

        Returns:
            bool: 検証結果
        """
        # 最低限必要なキーが存在するか確認
        required_keys = ["raceInfo", "racing"]
        if not all(key in position_data for key in required_keys):
            self.logger.warning(
                f"位置情報データに必要なキーがありません: {list(position_data.keys())}"
            )
            return False

        # レース情報の検証
        race_info = position_data.get("raceInfo", {})
        if not race_info or not isinstance(race_info, dict):
            self.logger.warning("レース基本情報が不正です")
            return False

        # レース周回データの検証
        racing_data = position_data.get("racing", [])
        if not racing_data or not isinstance(racing_data, list):
            self.logger.warning("レース周回データが不正です")
            return False

        return True

    def cleanup(self):
        """セッションのクリーンアップ処理"""
        if hasattr(self, "session") and self.session:
            try:
                self.session.close()
                self.logger.debug("セッションをクローズしました")
            except Exception as e:
                self.logger.error(f"セッションクローズ中にエラー: {str(e)}")

    def get_race_result(
        self,
        race_id: str,
        yenjoy_url: Optional[str] = None,
        race_info_for_yenjoy: Optional[Dict[str, Any]] = None,
    ):
        """
        レース結果を取得

        Args:
            race_id (str): Winticketのレース ID (ログや結果の紐付け用)
            yenjoy_url (str, optional): 直接指定するYenJoyの結果ページURL。
                                      これが指定されていれば優先的に使用される。
            race_info_for_yenjoy (dict, optional): YenJoy URL構築に必要な情報を含む辞書。
                                                   yenjoy_urlがNoneの場合に使用される。
                                                   必要なキー: 'race_date_yyyymmdd', 'venue_code', 'race_number'

        Returns:
            dict or None: レース結果データ、取得失敗時はNone
        """

        target_url = yenjoy_url
        yenjoy_race_id_for_log = "N/A"  # ログ用

        if not target_url:
            if not race_info_for_yenjoy or not all(
                k in race_info_for_yenjoy
                for k in [
                    "cup_start_date_yyyymmdd",  # 開催初日が必要
                    "race_date_yyyymmdd",
                    "venue_code",
                    "race_number",
                ]
            ):
                self.logger.error(
                    f"Race ID {race_id}: YenJoy URL構築に必要な情報が不足しています。必要なキー: cup_start_date_yyyymmdd, race_date_yyyymmdd, venue_code, race_number. 受け取ったキー: {list(race_info_for_yenjoy.keys()) if race_info_for_yenjoy else 'None'}"
                )
                return None

            try:
                # 新しいYenJoyのURL形式に合わせて構築
                # https://www.yen-joy.net/kaisai/race/result/{first_day(yyyyMM)}/{venue_id}/{first_day(yyyyMMdd)}/{race_day(yyyyMMdd)}/{race_number}
                cup_start_date = race_info_for_yenjoy["cup_start_date_yyyymmdd"]
                first_day_yyyymm = cup_start_date[:6]
                venue_id = str(race_info_for_yenjoy["venue_code"]).zfill(2)
                first_day_yyyymmdd = cup_start_date
                race_day_yyyymmdd = race_info_for_yenjoy["race_date_yyyymmdd"]
                race_num = str(race_info_for_yenjoy["race_number"]).zfill(2)

                target_url = f"{self.BASE_URL}/kaisai/race/result/{first_day_yyyymm}/{venue_id}/{first_day_yyyymmdd}/{race_day_yyyymmdd}/{race_num}"
                yenjoy_race_id_for_log = f"{first_day_yyyymm}_{venue_id}_{first_day_yyyymmdd}_{race_day_yyyymmdd}_{race_num}"  # ログ用IDも詳細化
                self.logger.info(
                    f"Race ID {race_id}: YenJoy URLを構築しました: {target_url}"
                )
            except Exception as e_url_build:
                self.logger.error(
                    f"Race ID {race_id}: YenJoy URL構築中にエラー: {e_url_build}",
                    exc_info=True,
                )
                return None
        else:
            # URLが直接指定された場合、ログ用のIDはURLから推測するか固定値
            yenjoy_race_id_for_log = target_url.split("/")[-3:]  # 末尾3要素程度
            yenjoy_race_id_for_log = "_ ".join(yenjoy_race_id_for_log)

        try:
            self.logger.info(
                f"レース {race_id} (Yenjoy Ref: {yenjoy_race_id_for_log}) の結果情報をHTMLから取得します。URL: {target_url}"
            )

            # HTMLを取得 (get_html_content を使用)
            html_response = self.get_html_content(
                target_url
            )  # get_html_content を呼び出す

            if (
                not html_response
                or not html_response.get("success")
                or not html_response.get("content")
            ):
                error_msg = (
                    html_response.get("error", "HTML取得失敗")
                    if html_response
                    else "HTML取得失敗"
                )
                status_code = (
                    html_response.get("status_code", "N/A") if html_response else "N/A"
                )
                self.logger.warning(
                    f"結果情報の取得に失敗しました: {error_msg}, Status: {status_code}, URL: {target_url}"
                )
                return None

            html_content = html_response["content"]

            if not html_content:
                self.logger.warning(
                    f"Race ID {race_id} (YenJoy: {yenjoy_race_id_for_log}): HTMLコンテンツが空です。URL: {target_url}"
                )
                return None

            self.logger.info(
                f"Race ID {race_id} (YenJoy: {yenjoy_race_id_for_log}): HTML取得成功、解析を開始します。 URL: {target_url}"
            )

            try:
                soup = BeautifulSoup(html_content, "html.parser")

                race_results = self._extract_race_results_from_html(soup)

                # 周回データ（HTML）を抽出
                lap_html_data = self._extract_lap_data_from_html(soup)

                # レース評を抽出
                race_comment = self._extract_race_comment_from_html(soup)

                # 検車場レポートを取得 (HTMLから)
                inspection_report_html = self._extract_inspection_report_from_html(
                    soup
                )  # メソッド名変更示唆

                # 結果データを構築
                result = {
                    "race_id": race_id,  # WinticketのIDを保持
                    "yenjoy_url": target_url,
                    "html_results": race_results,
                    "lap_html_data": lap_html_data,
                }

                if race_comment:
                    result["race_comment"] = race_comment

                if inspection_report_html:
                    result["inspection_report"] = inspection_report_html

                self.logger.info(f"レース {race_id} の結果情報を取得しました")
                return result

            except Exception as e:
                self.logger.error(
                    f"レース結果データの取得中にエラーが発生しました (RaceID: {race_id}, YenJoyRef: {yenjoy_race_id_for_log}): {str(e)}",
                    exc_info=True,
                )
                return None

        except Exception as e:
            self.logger.error(
                f"レース結果データの取得中にエラーが発生しました (RaceID: {race_id}, YenJoyRef: {yenjoy_race_id_for_log}): {str(e)}",
                exc_info=True,
            )
            return None

    def get_html_content(self, url: str, retry_count: int = 3) -> Dict[str, Any]:
        """
        指定されたURLからHTMLコンテンツを取得する。
        リトライ処理、エラーハンドリングを含む。

        Args:
            url (str): 取得対象のURL
            retry_count (int): リトライ回数

        Returns:
            dict: {'success': bool, 'content': Optional[str], 'status_code': Optional[int], 'error': Optional[str]}
        """
        for attempt in range(retry_count):
            try:
                self._throttle_request()  # APIリクエストのスロットリング
                self.logger.debug(
                    f"HTML取得リクエスト実行 (試行 {attempt + 1}/{retry_count}): {url}"
                )
                start_time = time.time()

                response = self.session.get(url, timeout=30)
                elapsed = time.time() - start_time
                self.logger.debug(
                    f"HTMLレスポンス受信: {url} (ステータス: {response.status_code}, 時間: {elapsed:.2f}秒)"
                )

                if response.status_code == 200:
                    # エンコーディングを正しく処理しようと試みる
                    content = response.content  # バイト列で取得
                    detected_encoding = (
                        response.encoding
                    )  # requestsが推測したエンコーディング
                    try:
                        # UTF-8でまず試す
                        html_text = content.decode("utf-8")
                    except UnicodeDecodeError:
                        self.logger.warning(
                            f"URL {url} のUTF-8デコード失敗。'{detected_encoding}' を試みます。"
                        )
                        try:
                            # requestsが推測したエンコーディング、またはShift_JIS, EUC-JPを試す
                            fallback_encodings = [
                                detected_encoding,
                                "shift_jis",
                                "euc_jp",
                            ]
                            for enc in fallback_encodings:
                                if enc:
                                    try:
                                        html_text = content.decode(enc)
                                        self.logger.info(
                                            f"URL {url} をエンコーディング '{enc}' でデコード成功。"
                                        )
                                        break
                                    except (UnicodeDecodeError, LookupError):
                                        continue
                            else:
                                self.logger.error(
                                    f"URL {url} のHTMLデコードに失敗しました。レスポンスヘッダのエンコーディング: {response.headers.get('content-type')}"
                                )
                                return {
                                    "success": False,
                                    "content": None,
                                    "status_code": response.status_code,
                                    "error": "HTML decode error",
                                }
                        except Exception as e_dec:
                            self.logger.error(
                                f"URL {url} のHTMLフォールバックデコード中にエラー: {e_dec}"
                            )
                            return {
                                "success": False,
                                "content": None,
                                "status_code": response.status_code,
                                "error": f"HTML decode fallback error: {e_dec}",
                            }

                    return {
                        "success": True,
                        "content": html_text,
                        "status_code": response.status_code,
                        "error": None,
                    }
                else:
                    self.logger.warning(
                        f"HTML取得エラー: {url} (ステータス: {response.status_code}) (試行 {attempt + 1}/{retry_count})"
                    )
                    # リトライ判断 (make_api_requestと同様のロジック)
                    if response.status_code == 429:  # レート制限
                        retry_after = int(response.headers.get("Retry-After", 60))
                        self.logger.warning(
                            f"レート制限エラー。{retry_after}秒後にリトライします。"
                        )
                        time.sleep(retry_after)
                    elif response.status_code >= 500:  # サーバーエラー
                        retry_wait = (attempt + 1) * 5  # 指数バックオフ
                        self.logger.warning(
                            f"サーバーエラー。{retry_wait}秒後にリトライします。"
                        )
                        time.sleep(retry_wait)
                    else:
                        # 4xxエラー (429除く) や 3xx リダイレクトなどはリトライしない
                        return {
                            "success": False,
                            "content": None,
                            "status_code": response.status_code,
                            "error": f"HTTP error {response.status_code}",
                        }

            except requests.RequestException as e:
                self.logger.error(
                    f"HTML取得リクエスト例外 (試行 {attempt + 1}/{retry_count}): {url} - {str(e)}"
                )
                retry_wait = (attempt + 1) * 5  # 指数バックオフ
                self.logger.warning(f"通信エラー。{retry_wait}秒後にリトライします。")
                time.sleep(retry_wait)
            except Exception as e_gen:
                self.logger.error(
                    f"HTML取得中に予期せぬエラー (試行 {attempt + 1}/{retry_count}): {url} - {str(e_gen)}",
                    exc_info=True,
                )
                return {
                    "success": False,
                    "content": None,
                    "status_code": None,
                    "error": f"Unexpected error: {str(e_gen)}",
                }

        self.logger.error(
            f"HTML取得の全てのリトライ ({retry_count}回) が失敗しました: {url}"
        )
        return {
            "success": False,
            "content": None,
            "status_code": None,
            "error": "All retries failed",
        }

    def _extract_race_results_from_html(self, soup):
        """
        HTMLから着順情報を抽出

        Args:
            soup (BeautifulSoup): HTMLパース結果

        Returns:
            list: 着順情報のリスト
        """
        try:
            results = []

            # 結果テーブルを探す (着、車番、印、選手名などの項目があるテーブル)
            result_table = None
            for table in soup.find_all("table"):
                headers = table.find_all("th")
                header_texts = [h.get_text(strip=True).lower() for h in headers]
                if (
                    "着" in header_texts
                    and "車番" in header_texts
                    and "選手名" in header_texts
                ):
                    result_table = table
                    break

            if not result_table:
                self.logger.warning("着順情報のテーブルが見つかりません")
                return []

            # 結果行を取得
            rows = result_table.find_all("tr")

            # ヘッダー行をスキップ
            for row in rows:
                # 上位2行はヘッダーなのでスキップ（通常）
                if row.find("th") and not row.find("td"):
                    continue

                # 選手データの各列を取得
                cells = row.find_all("td")
                if len(cells) < 10:  # 最低限必要なカラム数
                    continue

                # テーブルの列構造に基づいてデータを抽出
                rank_text = cells[0].get_text(strip=True)

                # 車番のアイコンから取得
                bracket_element = cells[1].find(
                    "i", class_=lambda c: c and "bikeno-" in c
                )
                bracket_number = 0
                if bracket_element:
                    bracket_classes = bracket_element.get("class", [])
                    for cls in bracket_classes:
                        if cls.startswith("bikeno-"):
                            bracket_number = int(cls.split("-")[1])
                            break

                # テーブルの残りのデータを抽出
                mark = cells[2].get_text(strip=True)

                # 選手名
                player_name_cell = cells[3]
                player_link = player_name_cell.find("a")
                player_name = (
                    player_link.get_text(strip=True)
                    if player_link
                    else player_name_cell.get_text(strip=True)
                )

                # プレイヤーIDの抽出
                player_id = ""
                if player_link and "href" in player_link.attrs:
                    player_id_match = re.search(
                        r"/racer/data/(\d+)", player_link["href"]
                    )
                    if player_id_match:
                        player_id = player_id_match.group(1)

                # 基本情報
                age = cells[4].get_text(strip=True)
                prefecture = cells[5].get_text(strip=True)
                period = cells[6].get_text(strip=True)
                class_rank = cells[7].get_text(strip=True)

                # 差（着差）
                diff = cells[8].get_text(strip=True)

                # 上りタイムと決まり手
                last_lap = cells[9].get_text(strip=True)
                winning_technique = (
                    cells[10].get_text(strip=True) if len(cells) > 10 else ""
                )

                # 走法とS/J/H/B
                symbols = cells[11].get_text(strip=True) if len(cells) > 11 else ""

                # 勝敗因
                win_factor = cells[12].get_text(strip=True) if len(cells) > 12 else ""

                # 個人状況
                personal_status = (
                    cells[13].get_text(strip=True) if len(cells) > 13 else ""
                )

                # 直近成績データ
                recent_results = []
                if len(cells) > 14:
                    for i in range(14, min(len(cells), 27)):  # 最大13項目の成績
                        recent_results.append(cells[i].get_text(strip=True))

                # 順位を数値に変換
                rank = 0
                if rank_text.isdigit():
                    rank = int(rank_text)
                elif rank_text == "落":
                    rank = 99  # 落車は99とする

                # タイムを抽出（上りタイム）
                try:
                    time_value = (
                        float(last_lap) if last_lap and last_lap != "0.0" else 0.0
                    )
                except ValueError:
                    time_value = 0.0

                # 着順データを作成
                result = {
                    "rank": rank,
                    "rank_text": rank_text,  # 生のテキスト（落車の場合など）
                    "bracketNumber": bracket_number,
                    "mark": mark,
                    "playerName": player_name,
                    "playerId": player_id,
                    "age": age,
                    "prefecture": prefecture,
                    "period": period,
                    "class": class_rank,
                    "diff": diff,
                    "time": time_value,
                    "lastLapTime": last_lap,
                    "winningTechnique": winning_technique,
                    "symbols": symbols,
                    "winFactor": win_factor,
                    "personalStatus": personal_status,
                    "recentResults": recent_results,
                }

                results.append(result)

            # 着順でソート
            results.sort(key=lambda x: x["rank"])

            return results

        except Exception as e:
            self.logger.error(
                f"着順情報の抽出中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return []

    def _extract_lap_data_from_html(self, soup):
        """
        HTMLから周回データを抽出（車番ごとの位置情報）

        Args:
            soup (BeautifulSoup): HTMLパース結果

        Returns:
            dict: 周回ごとの選手位置情報
        """
        try:
            lap_sections = {}
            # 周回データの各セクションを取得 (b-hyoクラスのdiv要素)
            sections = soup.find_all("div", class_="b-hyo")

            for section_idx, section in enumerate(sections):
                # セクション名を取得
                section_header = section.find("th", class_="bg-base-color")
                if not section_header:
                    continue

                section_name = "".join(section_header.stripped_strings)

                # セクション内の車番と位置情報を取得
                positions = []
                bike_icons = section.find_all(
                    "span", class_=lambda c: c and "bike-icon-wrapper" in c
                )

                for bike_icon in bike_icons:
                    # 車番、x座標、y座標を取得
                    classes = bike_icon.get("class", [])
                    bike_no = None
                    x_val = None
                    y_val = None
                    arrow = False

                    for cls in classes:
                        if cls.startswith("bikeno-"):
                            bike_no = cls.split("-")[1]
                        elif cls.startswith("x-"):
                            x_val = int(cls.split("-")[1])
                        elif cls.startswith("y-"):
                            y_val = int(cls.split("-")[1])

                    # 選手名を取得
                    racer_name_span = bike_icon.find("span", class_="racer-nm")
                    racer_name = (
                        racer_name_span.get_text(strip=True) if racer_name_span else ""
                    )

                    # 進行方向を示す矢印があるかチェック
                    bike_icon_elem = bike_icon.find("span", class_="bike-icon")
                    if bike_icon_elem and "arrow" in bike_icon_elem.get("class", []):
                        arrow = True

                    # フルネームを取得（ツールチップから）
                    full_name = racer_name
                    tooltip = bike_icon.find(
                        "div", class_=lambda c: c and "tooltip-content" in c
                    )
                    if tooltip:
                        full_name_li = tooltip.find(
                            "li",
                            class_=lambda c: c
                            and "not-grow" in c
                            and "img" not in str(c),
                        )
                        if full_name_li:
                            full_name = full_name_li.get_text(strip=True).replace(
                                "\u00a0", " "
                            )  # &nbsp;を通常のスペースに変換

                    # 有効なデータのみ追加
                    if bike_no and x_val is not None and y_val is not None:
                        positions.append(
                            {
                                "bike_no": bike_no,
                                "racer_name": racer_name,
                                "full_name": full_name,
                                "x": x_val,
                                "y": y_val,
                                "arrow": arrow,
                            }
                        )

                # セクション情報をラップデータに追加
                if positions:
                    lap_sections[section_name] = positions

            # 結果を取得
            return lap_sections

        except Exception as e:
            self.logger.error(
                f"周回データの抽出中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return {}

    def _extract_race_comment_from_html(self, soup):
        """
        HTMLからレース評を抽出

        Args:
            soup (BeautifulSoup): HTMLパース結果

        Returns:
            str: レース評
        """
        try:
            # レース評を探す (「レース評」というタイトルを持つ要素)
            comment_header = soup.find(
                ["h3", "h4"], text=lambda t: t and "レース評" in t
            )
            if not comment_header:
                # 別の探し方を試す
                comment_div = soup.find(
                    "div", class_=lambda c: c and "race-comment" in c
                )
                if comment_div:
                    comment_text = comment_div.get_text(strip=True)
                    return comment_text
                return None

            # レース評の親要素を取得
            comment_block = comment_header.find_parent("div")
            if not comment_block:
                return None

            # レース評のテキストを取得 (pタグやdivタグの内容)
            comment_texts = []
            for p in comment_block.find_all(["p", "div"]):
                # hタグは除外 (既に見つけたレース評のタイトルなど)
                if not p.find(["h1", "h2", "h3", "h4", "h5", "h6"]):
                    text = p.get_text(strip=True)
                    if text and "レース評" not in text:  # タイトルを除外
                        comment_texts.append(text)

            # 複数のテキストをつなげる
            if comment_texts:
                return " ".join(comment_texts)

            return None

        except Exception as e:
            self.logger.error(
                f"レース評の抽出中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return None

    def _extract_inspection_report_from_html(self, soup):
        """
        HTMLから検車場レポートを抽出 (get_inspection_report から分離)
        """
        try:
            report_data = []
            # 検車場レポートセクションを探す (複数の可能性を考慮)
            report_section_h3 = soup.find(
                "h3", text=lambda t: t and "検車場レポート" in t
            )
            report_section_div = soup.find(
                "div",
                class_=lambda c: c
                and ("kensya" in c.lower() or "KenshaReport_section" in c),
            )

            report_block = None
            if report_section_h3:
                report_block = report_section_h3.find_parent("div")
            elif report_section_div:
                report_block = report_section_div

            if not report_block:
                self.logger.info(
                    "検車場レポートセクションが見つかりません (HTMLパース)。"
                )
                return []  # 空リストを返す

            player_comments = {}
            # パターン1: KenshaReport_section__ 内の result-kensyajyou-report-wrap
            player_blocks_v1 = report_block.find_all(
                "div", class_="result-kensyajyou-report-wrap"
            )
            if player_blocks_v1:
                for block in player_blocks_v1:
                    player_name_tag = block.find(["h4", "dt"])
                    comment_tag = block.find(["p", "dd"])
                    if player_name_tag and comment_tag:
                        player_name = player_name_tag.get_text(strip=True)
                        comment = comment_tag.get_text(strip=True)
                        if player_name and comment:
                            player_comments[player_name] = comment

            # パターン2: result-kensya 直下の p.result-kensya-report-text (【選手名】コメント形式)
            if not player_comments:  # まだ見つかっていなければ
                report_p_tags = report_block.find_all(
                    "p", class_="result-kensya-report-text"
                )
                for p_tag in report_p_tags:
                    comment_blob = p_tag.get_text(separator="\n", strip=True)
                    found_in_blob = re.findall(r"【([^】]+)】([^【]+)", comment_blob)
                    for name, comm in found_in_blob:
                        player_comments[name.strip()] = comm.strip()

            # パターン3: h4/p や dt/dd のシーケンス
            if not player_comments:  # まだ見つかっていなければ
                current_player_name = None
                for tag in report_block.find_all(
                    ["h4", "p", "dt", "dd"], recursive=False
                ):
                    if tag.name in ["h4", "dt"]:
                        current_player_name = tag.get_text(strip=True)
                    elif tag.name in ["p", "dd"] and current_player_name:
                        comment = tag.get_text(strip=True)
                        comment = re.sub(
                            r"^【.*?】\s*", "", comment
                        )  # プレフィックス除去
                        if current_player_name and comment:
                            player_comments[current_player_name] = comment
                        current_player_name = None  # 一度読んだらリセット

            for player, comment in player_comments.items():
                report_data.append(
                    {"player_name_full": player, "comment_text": comment}
                )

            if not report_data:
                self.logger.info(
                    "検車場レポートの具体的なコメントが見つかりません (HTMLパース)。"
                )
            else:
                self.logger.info(
                    f"検車場レポートを {len(report_data)} 件抽出しました (HTMLパース)。"
                )
            return report_data
        except Exception as e:
            self.logger.error(f"検車場レポートの抽出中にエラー: {e}", exc_info=True)
            return []  # エラー時は空リスト
