"""
ステップ5: Yenjoyのレース位置情報と周回データ取得API
"""

import logging
import re
import time

import requests

# ユーザーエージェントの基本形
USER_AGENT_BASE = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/91.0.4472.124 Safari/537.36"
)
USER_AGENT = f"{USER_AGENT_BASE} YenjoyApp/0.1.0"


class YenjoyStep5API:
    """
    ステップ5: Yenjoyのレース位置情報と周回データを取得するAPI
    """

    # API基本URL
    BASE_URL = "https://yenjoy.keirin.jp"

    # APIエンドポイント
    ENDPOINTS = {
        "position_api": "/race-positions/api/{race_id}/positions.json",
        "race_detail": "/race-detail/",
    }

    def __init__(
        self, race_id_mapper=None, rate_limiter=None, logger=None, session=None
    ):
        """
        初期化

        Args:
            race_id_mapper: レースID変換用のインスタンス（WinticketのレースIDをYenjoyのレースIDに変換する機能）
            rate_limiter: APIレート制限用のインスタンス
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
            session: 既存のrequestsセッション（省略時は新規作成）
        """
        self.logger = logger or logging.getLogger(__name__)
        self.rate_limiter = rate_limiter
        self.race_id_mapper = race_id_mapper

        # セッション初期化（引数で指定されていればそれを使用、なければ新規作成）
        self.session = session or requests.Session()

        # ユーザーエージェント設定
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "https://yenjoy.keirin.jp",
                "Referer": "https://yenjoy.keirin.jp/",
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

    def _convert_to_yenjoy_race_id(self, winticket_race_id, date_str=None):
        """
        WinticketのレースIDをYenjoyのレースIDに変換

        Args:
            winticket_race_id (str): WinticketのレースID
            date_str (str, optional): 日付文字列（YYYYMMDD形式）

        Returns:
            str or None: YenjoyのレースID
        """
        try:
            # IDマッパーが設定されている場合はそれを使用
            if self.race_id_mapper:
                return self.race_id_mapper.convert_to_yenjoy_id(
                    winticket_race_id, date_str
                )

            # パターンに一致するか確認
            pattern = r"^(.*)_(\d+)_(\d+)$"
            match = re.match(pattern, winticket_race_id)

            if match:
                # cup_id = match.group(1) # 未使用のため削除
                # schedule_index = int(match.group(2)) # 未使用のため削除
                race_number = int(match.group(3))

                # 日付を取得できればそれを使用
                if date_str and re.match(r"^\d{8}$", date_str):
                    year = date_str[:4]
                    month = date_str[4:6]
                    day = date_str[6:8]

                    # Yenjoy形式のIDに変換
                    # フォーマット: YYYYMMDD + 会場コード + レース番号
                    # 注意: これは仮の実装です。実際のマッピングは異なる場合があります。
                    venue_code = "00"  # 仮の値
                    return f"{year}{month}{day}{venue_code}{race_number:02d}"

                self.logger.warning(
                    f"日付情報がないため、YenjoyレースIDへの変換が不完全です: {winticket_race_id}"
                )
                return None

            self.logger.error(f"レースID形式が不正です: {winticket_race_id}")
            return None

        except Exception as e:
            self.logger.error(
                f"YenjoyレースIDへの変換中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return None

    def get_race_position_data(self, race_id, date_str=None):
        """
        レースの位置情報データを取得

        Args:
            race_id (str): Winticketのレース ID
            date_str (str, optional): 日付文字列（YYYYMMDD形式）

        Returns:
            dict or None: レースの位置情報データ、取得失敗時はNone
        """
        # Yenjoy の race_id に変換
        yenjoy_race_id = self._convert_to_yenjoy_race_id(race_id, date_str)
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

            self.logger.info(f"レース {race_id} の位置情報を取得しました")
            return position_data

        except Exception as e:
            self.logger.error(
                f"レース位置情報の取得中にエラーが発生しました: {str(e)}", exc_info=True
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

    def get_race_lap_data(self, race_id, date_str=None):
        """
        レースの周回データを取得

        Args:
            race_id (str): Winticketのレース ID
            date_str (str, optional): 日付文字列（YYYYMMDD形式）

        Returns:
            dict or None: レースの周回データ、取得失敗時はNone
        """
        # 位置情報データを取得
        position_data = self.get_race_position_data(race_id, date_str)
        if not position_data:
            return None

        # 周回データを抽出
        try:
            # レース周回データ
            racing_data = position_data.get("racing", [])

            # レース全体の周回データ
            race_laps = {"laps": []}

            # 選手ごとの周回データ
            racer_laps = {"racer_laps": []}

            # 選手情報を抽出
            racers = {}
            for racer in position_data.get("racers", []):
                racer_id = str(racer.get("id", ""))
                racers[racer_id] = {
                    "playerId": racer_id,
                    "name": racer.get("name", ""),
                    "frameNumber": racer.get("frameNumber", 0),
                    "laps": [],
                }

            # 周回データを構築
            for lap_data in racing_data:
                lap_number = lap_data.get("lap", 0)
                lap_time = lap_data.get("lapTime", "")
                total_time = lap_data.get("totalTime", "")
                speed = lap_data.get("speed", 0)

                # レース全体の周回データに追加
                race_laps["laps"].append(
                    {
                        "lapNumber": lap_number,
                        "lapTime": lap_time,
                        "totalTime": total_time,
                        "speed": speed,
                    }
                )

                # 選手ごとの周回データを抽出
                for racer_id, racer_pos in lap_data.get("positions", {}).items():
                    if racer_id in racers:
                        # 周回ごとの選手データを追加
                        racer_lap = {
                            "lapNumber": lap_number,
                            "lapTime": racer_pos.get("lapTime", ""),
                            "totalTime": racer_pos.get("totalTime", ""),
                            "speed": racer_pos.get("speed", 0),
                        }
                        racers[racer_id]["laps"].append(racer_lap)

            # 選手ごとの周回データをリストに変換
            for racer_id, racer_data in racers.items():
                racer_laps["racer_laps"].append(racer_data)

            lap_data = {"race_laps": race_laps, "racer_laps": racer_laps}

            self.logger.info(f"レース {race_id} の周回データを抽出しました")
            return lap_data

        except Exception as e:
            self.logger.error(
                f"レース周回データの抽出中にエラーが発生しました: {str(e)}",
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
