import logging
from datetime import datetime, timedelta

import requests

from api_rate_limiter import ApiRateLimiter
from keirin_config import get_config

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("winticket_api.log"), logging.StreamHandler()],
)
logger = logging.getLogger("WinticketAPI")


class WinticketAPI:
    def __init__(self, debug=False, config_file=None):
        """
        WinticketAPIクライアントの初期化

        Args:
            debug (bool): デバッグモードを有効にするかどうか
            config_file (str, optional): 設定ファイルのパス
        """
        self.base_url = "https://api.winticket.jp"
        self.debug = debug
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Referer": "https://keirin.winticket.jp/",
            "Connection": "keep-alive",
        }

        # デフォルト設定
        default_rate_limit = 1.0
        get_cups_rate_limit = 2.0
        get_cup_detail_rate_limit = 1.5
        get_race_detail_rate_limit = 1.5
        get_odds_rate_limit = 1.0

        # 設定を読み込み
        if config_file:
            try:
                self.config = get_config(config_file)
                winticket_config = self.config.get_winticket_config()

                # 設定から値を取得
                default_rate_limit = winticket_config["default_rate_limit"]
                get_cups_rate_limit = winticket_config["get_cups_rate_limit"]
                get_cup_detail_rate_limit = winticket_config[
                    "get_cup_detail_rate_limit"
                ]
                get_race_detail_rate_limit = winticket_config[
                    "get_race_detail_rate_limit"
                ]
                get_odds_rate_limit = winticket_config["get_odds_rate_limit"]
            except Exception as e:
                logger.warning(f"設定ファイル読み込みエラー: {e}")
                logger.warning("デフォルト値を使用します")

        # APIレート制限を設定
        self.rate_limiter = ApiRateLimiter(default_rate_limit=default_rate_limit)
        self.rate_limiter.set_rate_limit("get_cups", get_cups_rate_limit)
        self.rate_limiter.set_rate_limit("get_cup_detail", get_cup_detail_rate_limit)
        self.rate_limiter.set_rate_limit("get_race_detail", get_race_detail_rate_limit)
        self.rate_limiter.set_rate_limit("get_odds", get_odds_rate_limit)

        logger.info("WinticketAPIインスタンスを初期化しました")

    def _make_request(self, url, endpoint=None, method="GET", params=None, data=None):
        """
        APIリクエストを送信する

        Args:
            url (str): リクエストURL
            endpoint (str, optional): APIエンドポイント名（レート制限用）
            method (str): HTTPメソッド
            params (dict, optional): クエリパラメータ
            data (dict, optional): POSTデータ

        Returns:
            dict: レスポンスデータ
        """

        def request_func():
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                params=params,
                json=data,
                timeout=30,
            )
            response.raise_for_status()
            return response.json()

        # エンドポイントが指定されていない場合はデフォルト値を設定
        if endpoint is None:
            endpoint = "default"

        # レート制限を適用してリクエスト実行
        return self.rate_limiter.execute(request_func, endpoint)

    def get_monthly_cups(self, date_str=None):
        """
        月間のカップ一覧を取得する

        Args:
            date_str (str, optional): 日付 (YYYYMMDD形式)、指定がない場合は今日の日付

        Returns:
            dict: 月間カップ情報を含む辞書
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")

        # ベースURLを更新
        url = f"{self.base_url}/v1/keirin/cups"

        # クエリパラメータを設定
        params = {"date": date_str, "fields": "month", "pfm": "web"}

        try:
            response_data = self._make_request(url, endpoint="get_cups", params=params)

            if self.debug:
                logger.debug(f"月間カップ取得レスポンス: {response_data}")

            if "month" in response_data and "cups" in response_data["month"]:
                cups = response_data["month"]["cups"]
                logger.info(f"月間カップを{len(cups)}件取得しました")
            else:
                cups = []
                logger.info("月間カップ情報が見つかりませんでした")

            return response_data

        except Exception as e:
            logger.error(f"月間カップ取得エラー: {str(e)}")
            return {"month": {"cups": []}}

    def get_cup_detail(self, cup_id):
        """
        カップ詳細情報を取得する

        Args:
            cup_id (str): カップID

        Returns:
            dict: カップ詳細情報を含む辞書
        """
        # ベースURLを更新
        url = f"{self.base_url}/v1/keirin/cups/{cup_id}"

        # クエリパラメータを設定
        params = {"pfm": "web"}

        try:
            response_data = self._make_request(
                url, endpoint="get_cup_detail", params=params
            )

            if self.debug:
                logger.debug(f"カップ詳細取得レスポンス: {response_data}")

            logger.info(f"カップ詳細情報を取得しました: ID {cup_id}")
            return response_data

        except Exception as e:
            logger.error(f"カップ詳細取得エラー: {str(e)}")
            return {}

    def get_race_detail_from_cup(self, cup_detail, race_number, day=1):
        """開催詳細情報から特定のレース詳細を抽出"""
        if not cup_detail:
            raise ValueError("開催詳細情報が取得できません")

        # 指定された日数のスケジュールを探す
        target_schedule = None
        for schedule in cup_detail.get("schedules", []):
            if schedule.get("index") == day:
                target_schedule = schedule
                break

        if not target_schedule:
            print("\n利用可能なスケジュール情報:")
            for schedule in cup_detail.get("schedules", []):
                print(
                    f"インデックス: {schedule.get('index')}, 開催日: {schedule.get('date')}, ID: {schedule.get('id')}"
                )
            raise ValueError(
                f"指定された日数（{day}日目）のスケジュールが見つかりません"
            )

        # 指定されたレース番号のレースを探す
        target_race = None
        for race in cup_detail.get("races", []):
            if (
                race.get("scheduleId") == target_schedule["id"]
                and race.get("number") == race_number
            ):
                target_race = race
                break

        if not target_race:
            print("\n利用可能なレース情報:")
            for race in cup_detail.get("races", []):
                if race.get("scheduleId") == target_schedule["id"]:
                    print(
                        f"レース番号: {race.get('number')}, 開催日: {race.get('date') or race.get('scheduleId')[:8]}, ステータス: {race.get('status')}"
                    )
            raise ValueError(f"指定されたレース（{race_number}レース）が見つかりません")

        print("\n対象レース情報:")
        print(f"レース番号: {target_race.get('number')}")
        print(f"開催日: {target_race.get('date') or target_race.get('scheduleId')[:8]}")
        print(f"ステータス: {target_race.get('status')}")

        # レースに関連するエントリー（出走）情報を抽出
        race_entries = []
        for entry in cup_detail.get("entries", []):
            if (
                entry.get("scheduleId") == target_schedule["id"]
                and entry.get("raceNumber") == race_number
            ):
                race_entries.append(entry)

        # レースに関連する選手情報を抽出
        player_ids = [entry.get("playerId") for entry in race_entries]
        race_players = []
        for player in cup_detail.get("players", []):
            if player.get("id") in player_ids:
                race_players.append(player)

        # 結果を構築
        result = {
            "race": target_race,
            "entries": race_entries,
            "players": race_players,
            "schedule": target_schedule,
        }

        return result

    def get_race_detail(self, cup_id, schedule_id, race_number, day=1):
        """
        レース詳細情報を取得する

        Args:
            cup_id (str): 開催ID
            schedule_id (int): スケジュールID (Cup Detail APIのschedules配列のindexプロパティを使用すること。idプロパティではない)
            race_number (int): レース番号
            day (int, optional): 開催日数（何日目か）

        Returns:
            dict: レース詳細情報
        """
        # 開催詳細から基本情報を取得して、初期化
        race_info = {}

        try:
            # 開催詳細から対象レースの基本情報を抽出（バックアップとして）
            cup_detail = self.get_cup_detail(cup_id)
            if cup_detail and "races" in cup_detail:
                race_info = self.get_race_detail_from_cup(cup_detail, race_number, day)
        except Exception as e:
            logger.warning(f"開催詳細からのレース情報抽出エラー: {str(e)}")

        # APIのURLを構築
        # 注意: schedule_idはCup Detail APIのschedules配列内のindexプロパティを使用すること。idプロパティではない
        url = f"{self.base_url}/v1/keirin/cups/{cup_id}/schedules/{schedule_id}/races/{race_number}"

        # クエリパラメータを設定
        params = {"fields": "race,entries,players,records,linePrediction", "pfm": "web"}

        # APIを呼び出し
        try:
            response = self._make_request(
                url, endpoint="get_race_detail", params=params
            )
            # APIからの応答がない場合は、開催詳細から抽出した情報を使用
            if not response or isinstance(response, dict) and not response:
                logger.warning(
                    "APIからの応答がありません。開催詳細から抽出した基本情報を使用します。"
                )
                return race_info
            return response
        except Exception as e:
            logger.error(f"レース詳細APIリクエストエラー: {str(e)}")
            logger.info("開催詳細から抽出した基本情報を使用します。")
            return race_info

    def get_race_odds(self, cup_id, schedule_id, race_number, day=1):
        """
        レースのオッズ情報を取得する

        Args:
            cup_id (str): 開催ID
            schedule_id (int): スケジュールID (Cup Detail APIのschedules配列のindexプロパティを使用すること。idプロパティではない)
            race_number (int): レース番号
            day (int, optional): 開催日数（何日目か）

        Returns:
            dict: オッズ情報
        """
        # APIのURLを構築
        # 注意: schedule_idはCup Detail APIのschedules配列内のindexプロパティを使用すること。idプロパティではない
        url = f"{self.base_url}/v1/keirin/cups/{cup_id}/schedules/{schedule_id}/races/{race_number}/odds"

        # クエリパラメータを設定
        params = {"pfm": "web"}

        logger.info(
            f"オッズ情報API呼び出し: cup_id={cup_id}, schedule_id={schedule_id}, race_number={race_number}"
        )

        # APIを呼び出し
        try:
            response = self._make_request(url, endpoint="get_race_odds", params=params)
            return response
        except Exception as e:
            logger.error(f"オッズ情報APIリクエストエラー: {str(e)}")
            # オッズ情報はAPIから取得できない場合はエラーとする
            raise

    def get_race_start_time(self, cup_id, schedule_id, race_number, day=1):
        """
        レースの発走時刻を取得

        Args:
            cup_id (str): 開催ID
            schedule_id (int): スケジュールID (Cup Detail APIのschedules配列のindexプロパティを使用すること。idプロパティではない)
            race_number (int): レース番号
            day (int, optional): 開催日数（何日目か）

        Returns:
            datetime: 発走時刻のdatetimeオブジェクト
        """
        try:
            # レース詳細情報を取得
            race_detail = self.get_race_detail(cup_id, schedule_id, race_number, day)

            # レース情報を取得
            race_info = race_detail.get("race", {})
            if not race_info and "race" in race_detail:
                race_info = race_detail["race"]

            # 発走時刻を取得
            start_time_str = race_info.get("startTime")

            if not start_time_str:
                print(
                    f"発走時刻情報がありません: cup_id={cup_id}, day={day}, race={race_number}"
                )
                return None

            # 日付情報を取得
            race_date = None
            if "date" in race_info:
                race_date = race_info["date"]
            elif "schedule" in race_detail and "date" in race_detail["schedule"]:
                race_date = race_detail["schedule"]["date"]

            if not race_date:
                # 日付情報がない場合はスケジュールIDから取得
                schedule_id_str = race_info.get("scheduleId", "")
                if len(schedule_id_str) >= 8:
                    race_date = schedule_id_str[:8]  # スケジュールIDの先頭8文字が日付

            if not race_date:
                print(
                    f"日付情報がありません: cup_id={cup_id}, day={day}, race={race_number}"
                )
                return None

            # 日付と時刻を組み合わせて日時オブジェクトを作成
            try:
                # 日付をYYYYMMDD形式に整形
                if len(race_date) == 8 and race_date.isdigit():
                    date_str = race_date
                else:
                    print(f"日付形式が不正: {race_date}")
                    return None

                # 時刻をHH:MM形式に整形
                if ":" in start_time_str:
                    time_str = start_time_str
                else:
                    # 時刻が数値のみの場合（例: 1410）
                    if len(start_time_str) == 4 and start_time_str.isdigit():
                        time_str = f"{start_time_str[:2]}:{start_time_str[2:]}"
                    else:
                        print(f"時刻形式が不正: {start_time_str}")
                        return None

                # 日時を結合してdatetimeオブジェクトに変換
                datetime_str = (
                    f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} {time_str}"
                )
                start_datetime = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")

                return start_datetime

            except Exception as e:
                print(f"日時変換エラー: {str(e)}")
                return None

        except Exception as e:
            print(f"発走時刻取得エラー: {str(e)}")
            return None

    def get_race_end_time(self, cup_id, schedule_id, race_number, day=1):
        """
        レースの終了時刻を取得

        Args:
            cup_id (str): 開催ID
            schedule_id (int): スケジュールID (Cup Detail APIのschedules配列のindexプロパティを使用すること。idプロパティではない)
            race_number (int): レース番号
            day (int, optional): 開催日数（何日目か）

        Returns:
            datetime: 終了時刻のdatetimeオブジェクト
        """
        start_time = self.get_race_start_time(cup_id, schedule_id, race_number, day)
        if not start_time:
            return None

        # 競輪レースは約2000m走るので、約2分で走行完了
        # 表彰式や確定までの時間を考慮して、発走から5分後を終了時刻と推定
        end_time = start_time + timedelta(minutes=5)
        return end_time

    def get_result_available_time(self, cup_id, schedule_id, race_number, day=1):
        """
        レース結果が利用可能になる時刻を推定（レース終了から15時間後）

        Args:
            cup_id (str): 開催ID
            schedule_id (int): スケジュールID (Cup Detail APIのschedules配列のindexプロパティを使用すること。idプロパティではない)
            race_number (int): レース番号
            day (int, optional): 開催日数（何日目か）

        Returns:
            datetime: 結果が利用可能になる時刻のdatetimeオブジェクト
        """
        end_time = self.get_race_end_time(cup_id, schedule_id, race_number, day)
        if not end_time:
            return None

        # レース終了から15時間後に結果が利用可能になると仮定
        available_time = end_time + timedelta(hours=15)
        return available_time

    def get_all_race_times(self, cup_id, day=1):
        """
        開催のすべてのレースの時刻情報を取得

        Args:
            cup_id (str): 開催ID
            day (int, optional): 開催日数（何日目か）

        Returns:
            list: 各レースの時刻情報をまとめたリスト
        """
        try:
            # 開催詳細を取得
            cup_detail = self.get_cup_detail(cup_id)
            if not cup_detail:
                print(f"開催詳細情報が取得できません: cup_id={cup_id}")
                return []

            # 指定された日程のスケジュールを取得
            target_schedule = None
            for schedule in cup_detail.get("schedules", []):
                if schedule.get("index") == day:
                    target_schedule = schedule
                    break

            if not target_schedule:
                print(
                    f"指定された日程のスケジュールが見つかりません: cup_id={cup_id}, day={day}"
                )
                return []

            # schedules配列からindexの値を取得
            # APIリクエストに使用するのはスケジュールのID(scheduleId)ではなく
            # schedules配列のindexプロパティ
            schedule_index = target_schedule.get("index")

            # スケジュールIDはレースの検索に使用（scheduleIdはraces配列の要素と紐づけるため）
            schedule_id = target_schedule.get("id")

            # 指定された日程のレース一覧を取得
            races = []
            for race in cup_detail.get("races", []):
                if race.get("scheduleId") == schedule_id:
                    races.append(race)

            # レース番号でソート
            races.sort(key=lambda x: x.get("number", 0))

            race_times = []
            for race in races:
                race_number = race.get("number")
                if not race_number:
                    continue

                race_info = {
                    "cup_id": cup_id,
                    "day": day,
                    "race_number": race_number,
                    "race_name": race.get("name", ""),
                    "status": race.get("status", ""),
                }

                # 発走時刻、終了時刻、結果利用可能時刻を取得
                # API呼び出しにはschedule_indexを使用
                start_time = self.get_race_start_time(
                    cup_id, schedule_index, race_number, day
                )
                if start_time:
                    race_info["start_time"] = start_time
                    race_info["start_time_str"] = start_time.strftime("%Y-%m-%d %H:%M")

                    end_time = self.get_race_end_time(
                        cup_id, schedule_index, race_number, day
                    )
                    if end_time:
                        race_info["end_time"] = end_time
                        race_info["end_time_str"] = end_time.strftime("%Y-%m-%d %H:%M")

                        available_time = self.get_result_available_time(
                            cup_id, schedule_index, race_number, day
                        )
                        if available_time:
                            race_info["result_available_time"] = available_time
                            race_info["result_available_time_str"] = (
                                available_time.strftime("%Y-%m-%d %H:%M")
                            )

                race_times.append(race_info)

            return race_times

        except Exception as e:
            print(f"レース時刻情報取得エラー: {str(e)}")
            return []
