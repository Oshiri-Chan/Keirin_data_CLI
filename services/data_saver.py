"""
データ保存サービス
"""

import logging
from datetime import datetime

import pandas as pd


class DataSaver:
    """
    データ保存を担当するクラス
    """

    def __init__(self, db_instance, logger=None):
        """
        初期化

        Args:
            db_instance: データベースインスタンス
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.db = db_instance
        self.logger = logger or logging.getLogger(__name__)

    def save_cups_data(self, data):
        """
        月間開催情報を保存（ステップ1）

        Args:
            data (dict): APIから取得した月間開催情報

        Returns:
            Tuple[bool, List[str]]: 成功したかどうか、取得した開催IDリスト
        """
        try:
            self.logger.info("月間開催情報（ステップ1）の保存を開始します")

            if not data or not isinstance(data, dict) or "month" not in data:
                self.logger.error("有効な開催情報がありません")
                return False, []

            month_data = data["month"]
            cup_ids = []

            # 地域情報の保存
            if "regions" in month_data and month_data["regions"]:
                regions_df = pd.DataFrame(
                    [
                        {
                            "region_id": region.get("id", ""),
                            "region_name": region.get("name", ""),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for region in month_data["regions"]
                    ]
                )

                self.logger.info(f"{len(regions_df)} 件の地域情報を保存します")

                # 一時ファイル経由で地域情報を保存
                regions_success = self.db.process_with_temp_file(
                    regions_df, "regions", ["region_id"], format="csv"
                )

                if not regions_success:
                    self.logger.error("地域情報の保存に失敗しました")

            # 会場情報の保存
            if "venues" in month_data and month_data["venues"]:
                venues_list = []

                for venue in month_data["venues"]:
                    venue_dict = {
                        "venue_id": venue.get("id", ""),
                        "venue_name": venue.get("name", ""),
                        "venue_short_name": venue.get("name1", ""),
                        "address": venue.get("address", ""),
                        "phone_number": venue.get("phoneNumber", ""),
                        "region_id": venue.get("regionId", ""),
                        "track_distance": venue.get("trackDistance", 0),
                        "bank_feature": venue.get("bankFeature", ""),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }

                    # 最高記録情報を追加
                    if "bestRecord" in venue and venue["bestRecord"]:
                        venue_dict.update(
                            {
                                "best_record_player_id": venue["bestRecord"].get(
                                    "playerId", ""
                                ),
                                "best_record_second": venue["bestRecord"].get(
                                    "second", 0
                                ),
                                "best_record_date": venue["bestRecord"].get("date", ""),
                            }
                        )

                    venues_list.append(venue_dict)

                venues_df = pd.DataFrame(venues_list)

                self.logger.info(f"{len(venues_df)} 件の会場情報を保存します")

                # 一時ファイル経由で会場情報を保存
                venues_success = self.db.process_with_temp_file(
                    venues_df, "venues", ["venue_id"], format="csv"
                )

                if not venues_success:
                    self.logger.error("会場情報の保存に失敗しました")

            # 開催情報の保存
            if "cups" in month_data and month_data["cups"]:
                cups_list = []

                for cup in month_data["cups"]:
                    # labelsが数値を含む場合があるので、すべて文字列に変換
                    labels = cup.get("labels", [])
                    if labels:
                        labels = [str(label) for label in labels]

                    cup_dict = {
                        "cup_id": str(cup.get("id", "")),
                        "cup_name": str(cup.get("name", "")),
                        "start_date": str(cup.get("startDate", "")),
                        "end_date": str(cup.get("endDate", "")),
                        "duration": int(cup.get("duration", 0)),
                        "grade": int(cup.get("grade", 0)),
                        "venue_id": str(cup.get("venueId", "")),
                        "labels": ",".join(labels),
                        "players_unfixed": int(cup.get("playersUnfixed", False)),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    cups_list.append(cup_dict)
                    cup_ids.append(cup.get("id", ""))

                cups_df = pd.DataFrame(cups_list)

                self.logger.info(f"{len(cups_df)} 件の開催情報を保存します")

                # 一時ファイル経由で開催情報を保存
                cups_success = self.db.process_with_temp_file(
                    cups_df, "cups", ["cup_id"], format="csv"
                )

                if not cups_success:
                    self.logger.error("開催情報の保存に失敗しました")
                    return False, []

            self.logger.info(
                f"月間開催情報（ステップ1）の保存が完了しました。取得した開催ID: {len(cup_ids)}件"
            )
            return True, cup_ids

        except Exception as e:
            self.logger.error(
                f"月間開催情報（ステップ1）の保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False, []

    def save_cup_detail(self, cup_id, cup_detail):
        """
        開催詳細情報を保存（ステップ2）

        Args:
            cup_id (str): 開催ID
            cup_detail (dict): APIから取得した開催詳細情報

        Returns:
            Tuple[bool, Dict[str, List[str]]]: 成功したかどうか、スケジュールとレースIDのマップ
        """
        try:
            self.logger.info(f"開催詳細情報（ステップ2）の保存を開始します: {cup_id}")

            if not cup_detail:
                self.logger.error(f"開催 {cup_id} の有効な詳細情報がありません")
                return False, {}

            schedule_race_map = {}

            # スケジュール情報の保存
            if "schedules" in cup_detail and cup_detail["schedules"]:
                schedules_df = pd.DataFrame(
                    [
                        {
                            "schedule_id": str(schedule.get("id", "")),
                            "date": str(schedule.get("date", "")),
                            "day": int(schedule.get("day", 0)),
                            "cup_id": str(schedule.get("cupId", "")),
                            "index": int(schedule.get("index", 0)),
                            "entries_unfixed": int(
                                schedule.get("entriesUnfixed", False)
                            ),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for schedule in cup_detail["schedules"]
                    ]
                )

                self.logger.info(
                    f"{len(schedules_df)} 件のスケジュール情報を保存します"
                )

                # 一時ファイル経由でスケジュール情報を保存
                schedules_success = self.db.process_with_temp_file(
                    schedules_df, "schedules", ["schedule_id"], format="csv"
                )

                if not schedules_success:
                    self.logger.error(
                        f"開催 {cup_id} のスケジュール情報の保存に失敗しました"
                    )

                # スケジュールIDを記録
                for schedule in cup_detail["schedules"]:
                    schedule_id = str(schedule.get("id", ""))
                    schedule_race_map[schedule_id] = []

            # レース情報の保存
            if "races" in cup_detail and cup_detail["races"]:
                races_list = []

                for race in cup_detail["races"]:
                    race_id = str(race.get("id", ""))
                    cup_id_from_race = str(race.get("cupId", ""))
                    schedule_id = str(race.get("scheduleId", ""))

                    race_dict = {
                        "race_id": race_id,
                        "race_number": int(race.get("number", 0)),
                        "race_name": str(race.get("name", "")),
                        "start_at": int(race.get("startAt", 0)),
                        "cup_id": cup_id_from_race,
                        "schedule_id": schedule_id,
                        "distance": int(race.get("distance", 0)),
                        "lap": int(race.get("lap", 0)),
                        "entries_number": int(race.get("entriesNumber", 0)),
                        "class": str(race.get("class", "")),
                        "race_type": str(race.get("raceType", "")),
                        "race_type3": str(race.get("raceType3", "")),
                        "is_grade_race": int(race.get("isGradeRace", False)),
                        "status": str(race.get("status", "")),
                        "weather": str(race.get("weather", "")),
                        "wind_speed": float(race.get("windSpeed", 0.0)),
                        "cancel": int(race.get("cancel", False)),
                        "cancel_reason": str(race.get("cancelReason", "")),
                        "close_at": int(race.get("closeAt", 0)),
                        "decided_at": int(race.get("decidedAt", 0)),
                        "has_digest_video": int(race.get("hasDigestVideo", False)),
                        "digest_video": str(race.get("digestVideo", "")),
                        "digest_video_provider": str(
                            race.get("digestVideoProvider", "")
                        ),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    races_list.append(race_dict)

                    # レースIDをスケジュールマップに追加
                    if schedule_id in schedule_race_map:
                        schedule_race_map[schedule_id].append(race_id)

                races_df = pd.DataFrame(races_list)

                self.logger.info(f"{len(races_df)} 件のレース情報を保存します")

                # 一時ファイル経由でレース情報を保存
                races_success = self.db.process_with_temp_file(
                    races_df, "races", ["race_id"], format="csv"
                )

                if not races_success:
                    self.logger.error(f"開催 {cup_id} のレース情報の保存に失敗しました")
                    return False, {}

            self.logger.info(f"開催詳細情報（ステップ2）の保存が完了しました: {cup_id}")
            return True, schedule_race_map

        except Exception as e:
            self.logger.error(
                f"開催詳細情報（ステップ2）の保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False, {}

    def save_winticket_race_data(
        self, race_id, date_str, race_info, entry_data, odds_data, result_data
    ):
        """
        Winticketのレースデータを保存（ステップ3）

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            race_info (dict): レース基本情報
            entry_data (dict): 出走表情報
            odds_data (dict): オッズ情報
            result_data (dict): 結果情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(f"レース {race_id} のデータ保存（ステップ3）を開始します")

            # レース基本情報の保存
            if race_info:
                # デバッグ用にrace_infoの内容をログに出力
                self.logger.debug(f"レース {race_id} の情報: {race_info}")

                # venue情報の処理を改善
                venue_id = race_info.get("venueId", "")
                venue_name = race_info.get("venueName", "")

                # venue_nameがない場合の対応
                if not venue_name:
                    if "venue" in race_info:
                        venue_name = race_info["venue"]
                    elif "venue_name" in race_info:
                        venue_name = race_info["venue_name"]
                    else:
                        venue_name = "不明" if not venue_id else f"会場ID:{venue_id}"

                race_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "race_date": date_str,
                            "venue_id": venue_id,
                            "venue_name": venue_name,
                            "race_number": race_info.get("raceNumber", 0),
                            "race_name": race_info.get("raceName", ""),
                            "race_type": race_info.get("raceType", ""),
                            "distance": race_info.get("distance", 0),
                            "start_time": race_info.get("startTime", ""),
                            "end_time": race_info.get("endTime", ""),
                            "is_finished": race_info.get("isFinished", 0),
                            "entry_count": race_info.get("entryCount", 0),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    ]
                )
                # 一時ファイル経由でレース情報を保存
                race_success = self.db.process_with_temp_file(
                    race_df, "races", ["race_id"], format="csv"
                )
                if not race_success:
                    self.logger.error(
                        f"レース {race_id} の基本情報の保存に失敗しました"
                    )
                    return False

            # 出走表情報の保存
            if entry_data and "entries" in entry_data:
                entries_df = pd.DataFrame(
                    [
                        {
                            "entry_id": entry.get("id", ""),
                            "race_id": race_id,
                            "rider_id": entry.get("playerId", ""),
                            "rider_name": entry.get("playerName", ""),
                            "frame_num": entry.get("frameNumber", 0),
                            "rank": (
                                entry.get("rank", 0)
                                if entry.get("rank") is not None
                                else 0
                            ),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for entry in entry_data["entries"]
                    ]
                )

                # 一時ファイル経由で出走表情報を保存
                entries_success = self.db.process_with_temp_file(
                    entries_df, "entries", ["entry_id"], format="csv"
                )
                if not entries_success:
                    self.logger.error(
                        f"レース {race_id} の出走表情報の保存に失敗しました"
                    )
                    return False

            # オッズ情報の保存
            if odds_data and "odds" in odds_data:
                payouts_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "odds_type": odds_type,
                            "bet_number": bet_number,
                            "odds_value": odds_value,
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for odds_type, odds_values in odds_data["odds"].items()
                        for bet_number, odds_value in odds_values.items()
                    ]
                )

                # 一時ファイル経由でオッズ情報を保存
                odds_success = self.db.process_with_temp_file(
                    payouts_df,
                    "payouts",
                    ["race_id", "odds_type", "bet_number"],
                    format="csv",
                )
                if not odds_success:
                    self.logger.error(
                        f"レース {race_id} のオッズ情報の保存に失敗しました"
                    )
                    return False

            # 結果情報の保存
            if result_data:
                results_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "rider_id": result.get("playerId", ""),
                            "rank": result.get("rank", 0),
                            "time": result.get("time", ""),
                            "win_odds": result.get("winOdds", 0),
                            "place_odds": result.get("placeOdds", 0),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for result in result_data.get("results", [])
                    ]
                )

                # 一時ファイル経由で結果情報を保存
                results_success = self.db.process_with_temp_file(
                    results_df, "race_results", ["race_id", "rider_id"], format="csv"
                )
                if not results_success:
                    self.logger.error(
                        f"レース {race_id} の結果情報の保存に失敗しました"
                    )
                    return False

            self.logger.info(
                f"レース {race_id} のデータ保存（ステップ3）が完了しました"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"レース {race_id} のデータ保存（ステップ3）中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_winticket_odds_data(self, race_id, odds_data):
        """
        Winticketのオッズデータを保存（ステップ4）

        Args:
            race_id (str): レースID
            odds_data (dict): オッズ情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"レース {race_id} のオッズデータ保存（ステップ4）を開始します"
            )

            if not odds_data or "odds" not in odds_data:
                self.logger.error(f"レース {race_id} の有効なオッズデータがありません")
                return False

            # オッズ情報の保存
            payouts_list = []

            for odds_type, odds_values in odds_data["odds"].items():
                for bet_number, odds_value in odds_values.items():
                    payouts_list.append(
                        {
                            "race_id": race_id,
                            "odds_type": odds_type,
                            "bet_number": bet_number,
                            "odds_value": odds_value,
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )

            if not payouts_list:
                self.logger.warning(f"レース {race_id} にオッズデータがありません")
                return False

            payouts_df = pd.DataFrame(payouts_list)

            # 一時ファイル経由でオッズ情報を保存
            odds_success = self.db.process_with_temp_file(
                payouts_df,
                "payouts",
                ["race_id", "odds_type", "bet_number"],
                format="csv",
            )

            if not odds_success:
                self.logger.error(f"レース {race_id} のオッズ情報の保存に失敗しました")
                return False

            self.logger.info(
                f"レース {race_id} のオッズデータ保存（ステップ4）が完了しました: {len(payouts_list)}件のオッズを保存"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"レース {race_id} のオッズデータ保存（ステップ4）中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_yenjoy_lap_data(self, race_id, date_str, race_laps, racer_laps):
        """
        Yenjoyの周回データを保存（ステップ5）

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            race_laps (dict): レース周回データ
            racer_laps (dict): 選手周回データ

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"レース {race_id} の周回データ保存（ステップ5）を開始します"
            )

            # レース周回データの保存
            if race_laps and "laps" in race_laps:
                laps_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "lap_number": lap.get("lapNumber", 0),
                            "lap_time": lap.get("lapTime", ""),
                            "total_time": lap.get("totalTime", ""),
                            "speed": lap.get("speed", 0),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for lap in race_laps["laps"]
                    ]
                )

                # 一時ファイル経由でレース周回データを保存
                laps_success = self.db.process_with_temp_file(
                    laps_df, "race_lap_times", ["race_id", "lap_number"], format="csv"
                )
                if not laps_success:
                    self.logger.error(
                        f"レース {race_id} の周回データの保存に失敗しました"
                    )
                    return False

            # 選手周回データの保存
            if racer_laps and "racer_laps" in racer_laps:
                racer_laps_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "rider_id": racer.get("playerId", ""),
                            "lap_number": lap.get("lapNumber", 0),
                            "lap_time": lap.get("lapTime", ""),
                            "total_time": lap.get("totalTime", ""),
                            "speed": lap.get("speed", 0),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for racer in racer_laps["racer_laps"]
                        for lap in racer.get("laps", [])
                    ]
                )

                # 一時ファイル経由で選手周回データを保存
                racer_laps_success = self.db.process_with_temp_file(
                    racer_laps_df,
                    "racer_lap_times",
                    ["race_id", "rider_id", "lap_number"],
                    format="csv",
                )
                if not racer_laps_success:
                    self.logger.error(
                        f"レース {race_id} の選手周回データの保存に失敗しました"
                    )
                    return False

            self.logger.info(
                f"レース {race_id} の周回データ保存（ステップ5）が完了しました"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"レース {race_id} の周回データ保存（ステップ5）中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_position_data(self, race_id, date_str, position_data):
        """
        レース位置情報データを保存（ステップ5の一部）

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            position_data (dict): 位置情報データ

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"レース {race_id} の位置情報データ保存（ステップ5）を開始します"
            )

            if not position_data:
                self.logger.error(f"レース {race_id} の位置情報データがありません")
                return False

            # 位置情報データから周回データを構築
            if "positions" in position_data:
                # 周回データの構築と保存
                lap_times = position_data.get("lap_times", [])
                if lap_times:
                    lap_times_df = pd.DataFrame(
                        [
                            {
                                "race_id": race_id,
                                "lap_number": lap.get("lap_number", 0),
                                "lap_time": lap.get("lap_time", ""),
                                "total_time": lap.get("total_time", ""),
                                "updated_at": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }
                            for lap in lap_times
                        ]
                    )

                    # 一時ファイル経由でレース周回データを保存
                    lap_times_success = self.db.process_with_temp_file(
                        lap_times_df,
                        "race_lap_times",
                        ["race_id", "lap_number"],
                        format="csv",
                    )

                    if not lap_times_success:
                        self.logger.error(
                            f"レース {race_id} の周回データの保存に失敗しました"
                        )

                # 選手ごとの位置情報を保存
                positions = position_data.get("positions", [])
                if positions:
                    positions_list = []

                    for rider_id, rider_positions in positions.items():
                        for pos in rider_positions:
                            position_dict = {
                                "race_id": race_id,
                                "rider_id": rider_id,
                                "timestamp": pos.get("time", 0),
                                "x": pos.get("x", 0),
                                "y": pos.get("y", 0),
                                "lap": pos.get("lap", 0),
                                "speed": pos.get("speed", 0),
                                "updated_at": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }
                            positions_list.append(position_dict)

                    if positions_list:
                        positions_df = pd.DataFrame(positions_list)

                        # 一時ファイル経由で位置情報を保存
                        positions_success = self.db.process_with_temp_file(
                            positions_df,
                            "rider_positions",
                            ["race_id", "rider_id", "timestamp"],
                            format="csv",
                        )

                        if not positions_success:
                            self.logger.error(
                                f"レース {race_id} の位置情報の保存に失敗しました"
                            )
                            return False

            self.logger.info(
                f"レース {race_id} の位置情報データ保存（ステップ5）が完了しました"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"レース {race_id} の位置情報データ保存（ステップ5）中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False
