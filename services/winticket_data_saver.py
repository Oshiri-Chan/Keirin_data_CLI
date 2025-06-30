"""
Winticketデータ保存サービス
"""

import logging
from datetime import timezone  # noqa: F401
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple  # noqa: F401

import pandas as pd


class WinticketDataSaver:
    """
    Winticketのデータ保存を担当するクラス
    """

    def __init__(self, db_instance, data_saver=None, logger=None):
        """
        初期化

        Args:
            db_instance: データベースインスタンス
            data_saver: 共通データ保存インスタンス（省略時はNone）
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.db = db_instance
        self.data_saver = data_saver
        self.logger = logger or logging.getLogger(__name__)

    def save_step1_monthly_cups(self, monthly_data):
        """
        ステップ1: 月間の開催情報を保存

        Args:
            monthly_data (dict): 月間開催情報

        Returns:
            Tuple[bool, List[str]]: 成功したかどうか、取得した開催IDリスト
        """
        try:
            self.logger.info("ステップ1: 月間の開催情報の保存を開始します")

            # Step1Saverインスタンスを作成して使用
            from services.savers.step1_saver import Step1Saver

            step1_saver = Step1Saver(self.db, self.logger)
            success, cup_ids = step1_saver.save_monthly_cups(monthly_data)

            if success:
                self.logger.info(
                    f"ステップ1: 月間の開催情報を保存しました。開催ID数: {len(cup_ids)}件"
                )
            else:
                self.logger.error("ステップ1: 月間の開催情報の保存に失敗しました")

            return success, cup_ids
        except Exception as e:
            self.logger.error(
                f"ステップ1: 月間の開催情報の保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False, []

    def save_step2_cup_detail(self, cup_id, cup_detail):
        """
        ステップ2: 開催詳細情報を保存

        Args:
            cup_id (str): 開催ID
            cup_detail (dict): 開催詳細情報

        Returns:
            Tuple[bool, Dict[str, List[str]]]: 成功したかどうか、スケジュールとレースIDのマップ
        """
        try:
            self.logger.info(f"ステップ2: 開催詳細情報の保存を開始します: {cup_id}")

            if not cup_detail:
                self.logger.error(f"開催 {cup_id} の有効な詳細情報がありません")
                return False, {}

            # Step2Saverインスタンスを作成して使用
            from services.savers.step2_saver import Step2Saver

            step2_saver = Step2Saver(self.db, self.logger)
            success, race_ids = step2_saver.save_cup_details(cup_id, cup_detail)

            # スケジュールとレースIDのマップを作成
            schedule_race_map = {}
            if "schedules" in cup_detail and cup_detail["schedules"]:
                for schedule in cup_detail["schedules"]:
                    schedule_id = str(schedule.get("id", ""))
                    schedule_race_map[schedule_id] = []

            if "races" in cup_detail and cup_detail["races"]:
                for race in cup_detail["races"]:
                    race_id = str(race.get("id", ""))
                    schedule_id = str(race.get("scheduleId", ""))
                    if schedule_id in schedule_race_map:
                        schedule_race_map[schedule_id].append(race_id)

            if success:
                total_races = sum(len(races) for races in schedule_race_map.values())
                self.logger.info(
                    f"ステップ2: 開催詳細情報を保存しました。スケジュール数: {len(schedule_race_map)}件、レース数: {total_races}件"
                )
            else:
                self.logger.error(
                    f"ステップ2: 開催詳細情報の保存に失敗しました: {cup_id}"
                )

            return success, schedule_race_map
        except Exception as e:
            self.logger.error(
                f"ステップ2: 開催詳細情報の保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False, {}

    def save_step3_race_data(self, race_id, date_str, race_info, entry_data):
        """
        ステップ3: レース基本情報と出走表を保存

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            race_info (dict): レース基本情報
            entry_data (dict): 出走表情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"ステップ3: レース {race_id} の基本情報と出走表の保存を開始します"
            )

            # Step3Saverインスタンスを作成して使用
            from services.savers.step3_saver import Step3Saver

            step3_saver = Step3Saver(self.db, self.logger)

            # レース情報と出走表を保存
            combined_data = {"race": race_info, "entries": entry_data}
            success = step3_saver.save_race_info(race_id, combined_data)

            if success:
                entry_count = len(entry_data) if entry_data else 0
                self.logger.info(
                    f"ステップ3: レース {race_id} の基本情報と出走表を保存しました（出走数: {entry_count}件）"
                )
            else:
                self.logger.error(
                    f"ステップ3: レース {race_id} の基本情報と出走表の保存に失敗しました"
                )

            return success
        except Exception as e:
            self.logger.error(
                f"ステップ3: レース基本情報と出走表の保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_step4_race_odds(self, race_id, odds_data):
        """
        ステップ4: レースのオッズ情報を保存（MySQL対応）

        Args:
            race_id (str): レースID
            odds_data (dict): オッズ情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"ステップ4: レース {race_id} のオッズ情報の保存を開始します (MySQL対応)"
            )

            # MySQL用にKeirinDataAccessorを作成してStep4Saverを初期化
            from database.db_accessor import KeirinDataAccessor
            from services.savers.step4_saver import Step4Saver

            # 設定ファイルからMySQL接続情報を読み込み
            mysql_accessor = KeirinDataAccessor()
            step4_saver = Step4Saver(mysql_accessor, self.logger)

            # オッズデータの保存処理にレースIDを追加
            odds_data_with_race_id = dict(odds_data) if odds_data else {}
            odds_data_with_race_id["race_id"] = race_id

            # MySQLのStep4Saverのメソッドを使用してオッズデータを保存
            batch_size = 1000  # 適切なバッチサイズを設定
            success = step4_saver.save_all_odds_for_race(
                race_id, odds_data_with_race_id, batch_size
            )

            if success:
                self.logger.info(
                    f"ステップ4: レース {race_id} のオッズ情報をMySQLに保存しました"
                )
            else:
                self.logger.error(
                    f"ステップ4: レース {race_id} のオッズ情報のMySQL保存に失敗しました"
                )

            return success
        except Exception as e:
            self.logger.error(
                f"ステップ4: オッズ情報のMySQL保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_step5_result_data(self, race_id, result_data):
        """
        ステップ5: レース結果情報を保存

        Args:
            race_id (str): レースID
            result_data (dict): 結果情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"ステップ5: レース {race_id} の結果情報の保存を開始します"
            )

            # Step5Saverインスタンスを作成して使用
            from services.savers.step5_saver import Step5Saver

            step5_saver = Step5Saver(self.db, self.logger)
            success = step5_saver.save_race_result(race_id, result_data)

            if success:
                self.logger.info(
                    f"ステップ5: レース {race_id} の結果情報を保存しました"
                )
            else:
                self.logger.error(
                    f"ステップ5: レース {race_id} の結果情報の保存に失敗しました"
                )

            return success
        except Exception as e:
            self.logger.error(
                f"ステップ5: 結果情報の保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def get_missing_race_ids(self, cup_id):
        """
        指定された開催の未取得レースIDを取得

        Args:
            cup_id (str): 開催ID

        Returns:
            List[str]: 未取得のレースIDリスト
        """
        try:
            self.logger.info(f"開催 {cup_id} の未取得レースIDを検索します")
            missing_races = self.db.get_missing_race_ids(cup_id)
            self.logger.info(f"開催 {cup_id} の未取得レースID: {len(missing_races)}件")
            return missing_races
        except Exception as e:
            self.logger.error(
                f"開催 {cup_id} の未取得レースID検索中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return []

    def save_race_data(
        self, race_id, date_str, race_info, entry_data, odds_data, result_data
    ):
        """
        レースデータを保存

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
            self.logger.info(f"レース {race_id} のデータ保存を開始します")

            # レース情報の保存
            if race_info:
                # デバッグ用にrace_infoの内容をログに出力
                self.logger.debug(f"レース {race_id} の情報: {race_info}")

                # venue情報の取得を改善
                venue = "不明"
                if "venue_name" in race_info and race_info["venue_name"]:
                    venue = race_info["venue_name"]
                elif "venue" in race_info and race_info["venue"]:
                    venue = race_info["venue"]
                elif "venueId" in race_info and race_info["venueId"]:
                    # venue IDから名前を取得する処理があればここに追加
                    venue = f"会場ID:{race_info['venueId']}"

                # レース情報をDataFrameに変換
                race_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "date": date_str,
                            "venue": venue,  # 改善したvenue取得
                            "race_number": race_info.get("race_number", 0),
                            "title": race_info.get("race_name", ""),
                            "distance": race_info.get("distance", 0),
                            "race_class": race_info.get("race_type", ""),
                            "weather": "",  # 天候情報は別途取得が必要
                            "temperature": 0.0,  # 気温情報は別途取得が必要
                            "is_finished": race_info.get("status")
                            == 3,  # ステータス3はレース終了
                            "entry_count": race_info.get("entry_count", 0),
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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

                # レースステータスのログ出力
                status = race_info.get("status")
                if status == 3:
                    self.logger.info(
                        f"レース {race_id} は終了しています（ステータス: {status}）"
                    )
                else:
                    self.logger.info(
                        f"レース {race_id} は未完了です（ステータス: {status}）"
                    )

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

            self.logger.info(f"レース {race_id} のデータ保存が完了しました")
            return True

        except Exception as e:
            self.logger.error(
                f"レース {race_id} のデータ保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_cups_data(self, data):
        """
        Winticketの月間開催情報を保存

        Args:
            data (dict): APIから取得した月間開催情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info("月間開催情報の保存を開始します")

            if not data or not isinstance(data, dict) or "month" not in data:
                self.logger.error("有効な開催情報がありません")
                return False

            month_data = data["month"]

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
                        "venue_slug": venue.get("slug", ""),
                        "address": venue.get("address", ""),
                        "phone_number": venue.get("phoneNumber", ""),
                        "region_id": venue.get("regionId", ""),
                        "website_url": venue.get("websiteUrl", ""),
                        "twitter_account": venue.get("twitterAccountId", ""),
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
                                "best_record_seconds": venue["bestRecord"].get(
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
                cups_df = pd.DataFrame(
                    [
                        {
                            "cup_id": cup.get("id", ""),
                            "cup_name": cup.get("name", ""),
                            "start_date": cup.get("startDate", ""),
                            "end_date": cup.get("endDate", ""),
                            "duration": cup.get("duration", 0),
                            "grade": cup.get("grade", 0),
                            "venue_id": cup.get("venueId", ""),
                            "labels": ",".join(cup.get("labels", [])),
                            "players_unfixed": (
                                1 if cup.get("playersUnfixed", False) else 0
                            ),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for cup in month_data["cups"]
                    ]
                )

                self.logger.info(f"{len(cups_df)} 件の開催情報を保存します")

                # 一時ファイル経由で開催情報を保存
                cups_success = self.db.process_with_temp_file(
                    cups_df, "cups", ["cup_id"], format="csv"
                )

                if not cups_success:
                    self.logger.error("開催情報の保存に失敗しました")

            self.logger.info("月間開催情報の保存が完了しました")
            return True

        except Exception as e:
            self.logger.error(
                f"月間開催情報の保存中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return False

    def map_venue_id_to_name(self, venue_id):
        """
        会場IDから会場名を取得

        Args:
            venue_id (str): 会場ID

        Returns:
            str: 会場名（見つからない場合は「不明」または会場ID）
        """
        try:
            # データベースから会場情報を検索
            query = f"SELECT venue_name FROM venues WHERE venue_id = '{venue_id}'"
            result = self.db.execute_query(query)

            if result and len(result) > 0:
                return result[0][0]
            else:
                return f"会場ID:{venue_id}"

        except Exception as e:
            self.logger.error(f"会場ID {venue_id} の検索中にエラー: {str(e)}")
            return f"会場ID:{venue_id}"

    def read_from_json_export(self, table_name, query=None):
        """
        JSONエクスポートデータを読み込む

        Args:
            table_name (str): テーブル名
            query (str, optional): 条件クエリ文字列

        Returns:
            list: JSONデータのリスト
        """
        try:
            return self.db.read_from_json_export(table_name, query)
        except Exception as e:
            self.logger.error(f"JSONエクスポートデータの読み込み中にエラー: {e}")
            return []

    def bulk_save_step2_data(
        self,
        schedules_data: List[Dict],
        races_data: List[Dict],
        race_status_data: List[Dict],
    ) -> Tuple[bool, Dict]:
        """
        スケジュール、レース、レースステータスのデータを一括でDBに保存する
        race_status は INSERT OR IGNORE で挿入する
        """
        conn = None
        cursor = None
        saved_schedules = 0
        saved_races = 0
        saved_race_statuses = 0
        overall_success = True

        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            if not conn.in_transaction:
                conn.execute("BEGIN")

            # --- スケジュールの一括保存 (変更なし) ---
            if schedules_data:
                schedule_insert = """
                INSERT OR REPLACE INTO schedules
                (schedule_id, cup_id, date, day, entries_unfixed, schedule_index, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                """
                schedule_params = []
                for s in schedules_data:
                    schedule_params.append(
                        (
                            str(s.get("id")),
                            s.get("cup_id"),
                            s.get("date"),
                            s.get("day"),
                            1 if s.get("entriesUnfixed") else 0,
                            s.get("index"),
                        )
                    )
                try:
                    cursor.executemany(schedule_insert, schedule_params)
                    saved_schedules = len(schedule_params)
                    self.logger.info(
                        f"スケジュール {saved_schedules} 件を一括保存しました。"
                    )
                except Exception as e:
                    self.logger.error(
                        f"スケジュールの一括保存中にエラー: {e}. クエリ: {schedule_insert}"
                    )
                    overall_success = False

            # --- レースの一括保存 (変更なし) ---
            if races_data:
                race_insert = """
                INSERT OR REPLACE INTO races
                (race_id, schedule_id, cup_id, number, class, race_type, start_at, close_at,
                 status, cancel, cancel_reason, weather, wind_speed, race_type3, distance,
                 lap, entries_number, is_grade_race, has_digest_video, digest_video,
                 digest_video_provider, decided_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """
                race_params = []
                for r in races_data:
                    race_params.append(
                        (
                            str(r.get("id")),
                            str(r.get("scheduleId")),
                            r.get("cup_id"),
                            r.get("number"),
                            r.get("class"),
                            r.get("raceType"),
                            self._unix_to_datetime_str(r.get("startAt")),
                            self._unix_to_datetime_str(r.get("closeAt")),
                            r.get("status"),
                            1 if r.get("cancel") else 0,
                            r.get("cancelReason"),
                            r.get("weather"),
                            r.get("windSpeed"),
                            r.get("raceType3"),
                            r.get("distance"),
                            r.get("lap"),
                            r.get("entriesNumber"),
                            1 if r.get("isGradeRace") else 0,
                            1 if r.get("hasDigestVideo") else 0,
                            r.get("digestVideo"),
                            r.get("digestVideoProvider"),
                            self._unix_to_datetime_str(r.get("decidedAt")),
                        )
                    )
                try:
                    cursor.executemany(race_insert, race_params)
                    saved_races = len(race_params)
                    self.logger.info(f"レース {saved_races} 件を一括保存しました。")
                except Exception as e:
                    self.logger.error(
                        f"レースの一括保存中にエラー: {e}. クエリ: {race_insert}"
                    )
                    overall_success = False

            # --- ★ レースステータスの一括初期化 (INSERT OR IGNORE) --- ★
            if race_status_data:
                # カラム名は実際の race_status テーブル定義に合わせる (step5_status を削除)
                status_insert = """
                INSERT OR IGNORE INTO race_status
                (race_id, step3_status, step4_status, last_updated)
                VALUES (?, ?, ?, datetime('now'))
                """
                status_params = []
                for st in race_status_data:
                    status_params.append(
                        (
                            st.get("race_id"),
                            st.get("step3_status", "pending"),  # デフォルト値
                            st.get("step4_status", "pending"),  # デフォルト値
                            # step5_status を削除
                        )
                    )
                try:
                    # INSERT OR IGNORE なので既存データは無視される
                    cursor.executemany(status_insert, status_params)
                    # rowcount は INSERT IGNORE では信頼できないため、試行件数を記録
                    saved_race_statuses = len(status_params)
                    self.logger.info(
                        f"レースステータス初期値挿入試行 {saved_race_statuses} 件を実行しました (既存データは無視)。"
                    )
                except Exception as e:
                    self.logger.error(
                        f"レースステータスの初期値挿入中にエラー: {e}. クエリ: {status_insert}"
                    )
                    overall_success = False  # ステータス挿入失敗も全体失敗扱い
            # --- ★ レースステータス保存ここまで --- ★

            # トランザクションコミット (エラーがなければ)
            if overall_success:
                if conn.in_transaction:
                    conn.commit()
                    self.logger.info(
                        "Step2データの一括保存トランザクションをコミットしました。"
                    )
            else:
                if conn.in_transaction:
                    conn.rollback()
                    self.logger.warning(
                        "Step2データの一括保存中にエラーが発生したため、ロールバックしました。"
                    )

            # ★ 戻り値の辞書に saved_race_statuses を追加
            result_details = {
                "saved_schedules": saved_schedules,
                "saved_races": saved_races,
                "saved_race_statuses": saved_race_statuses,
                "message": (
                    "Bulk save completed."
                    if overall_success
                    else "Bulk save failed partially or fully."
                ),
            }
            return overall_success, result_details

        except Exception as e:
            self.logger.error(
                f"Step2データの一括保存処理中に予期せぬエラー: {e}", exc_info=True
            )
            if conn and conn.in_transaction:
                conn.rollback()
            # ★ エラー時の戻り値にも saved_race_statuses を追加
            return False, {
                "saved_schedules": 0,
                "saved_races": 0,
                "saved_race_statuses": 0,
                "message": f"Unexpected error: {e}",
            }
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.db.close_connection()  # Databaseクラスのメソッドでクローズ

    def _unix_to_datetime_str(self, timestamp: Any) -> Optional[str]:
        # Helper function - assuming it exists and works correctly
        if timestamp is None:
            return None
        try:
            dt_object = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            return dt_object.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, OSError):
            self.logger.warning(
                f"無効なタイムスタンプ形式: {timestamp}. None を使用します。"
            )
            return None

    def bulk_save_step3_data(
        self,
        players_data: list,
        entries_data: list,
        records_data: list,
        line_predictions_data: list,
    ) -> tuple[bool, dict]:
        """
        ステップ3のデータを一括保存 (Player, Entry, Record, LinePrediction)
        MySQL用に修正：KeirinDataAccessorを使用するStep3Saverを呼び出す

        Args:
            players_data (list): 保存するプレイヤーデータのリスト
            entries_data (list): 保存する出走データのリスト
            records_data (list): 保存する選手成績データのリスト
            line_predictions_data (list): 保存するライン予測データのリスト

        Returns:
            tuple[bool, dict]: (成功/失敗, 保存結果の詳細)
        """
        start_time = datetime.now()
        self.logger.info(
            f"Step3 Bulk Save 開始 (MySQL対応) - Players: {len(players_data)}, "
            f"Entries: {len(entries_data)}, "
            f"Records: {len(records_data)}, "
            f"Lines: {len(line_predictions_data)}"
        )
        try:
            from services.savers.step3_saver import Step3Saver
            from database.db_accessor import KeirinDataAccessor

            # MySQL用のKeirinDataAccessorを初期化
            mysql_accessor = KeirinDataAccessor(self.logger)
            step3_saver = Step3Saver(mysql_accessor, self.logger)

            # Step3Saverの新しいバッチ保存メソッドを使用
            # 既存のAPIでは、レースごとに個別のメソッドを呼ぶ必要があります
            total_saved_players = 0
            total_saved_entries = 0
            total_saved_records = 0
            total_saved_lines = 0
            overall_success = True

            # レースIDごとにデータをグループ化
            races_by_id = {}

            # プレイヤーデータからレースIDを抽出
            for player in players_data:
                race_id = player.get("race_id")
                if race_id:
                    if race_id not in races_by_id:
                        races_by_id[race_id] = {
                            "players": [],
                            "entries": [],
                            "records": [],
                            "lines": [],
                        }
                    races_by_id[race_id]["players"].append(player)

            # エントリーデータを追加
            for entry in entries_data:
                race_id = entry.get("race_id")
                if race_id and race_id in races_by_id:
                    races_by_id[race_id]["entries"].append(entry)

            # レコードデータを追加
            for record in records_data:
                race_id = record.get("race_id")
                if race_id and race_id in races_by_id:
                    races_by_id[race_id]["records"].append(record)

            # ラインデータを追加（一括なので簡略化）
            if line_predictions_data:
                for line in line_predictions_data:
                    race_id = line.get("race_id")
                    if race_id and race_id in races_by_id:
                        races_by_id[race_id]["lines"].append(line)

            # レースごとに保存
            batch_size = 100  # 適切なバッチサイズ
            for race_id, race_data in races_by_id.items():
                try:
                    # 個別保存メソッドを呼び出し
                    if race_data["players"]:
                        step3_saver.save_players_batch(
                            race_data["players"], race_id, batch_size
                        )
                        total_saved_players += len(race_data["players"])

                    if race_data["entries"]:
                        step3_saver.save_entries_batch(
                            race_data["entries"], race_id, batch_size
                        )
                        total_saved_entries += len(race_data["entries"])

                    if race_data["records"]:
                        step3_saver.save_player_records_batch(
                            race_data["records"], race_id, batch_size
                        )
                        total_saved_records += len(race_data["records"])

                    if race_data["lines"]:
                        # ライン予測は別処理（既存APIに合わせる）
                        for line_data in race_data["lines"]:
                            step3_saver.save_line_predictions_batch(line_data, race_id)
                            total_saved_lines += 1

                except Exception as e_race:
                    self.logger.error(
                        f"レースID {race_id} の保存中にエラー: {e_race}", exc_info=True
                    )
                    overall_success = False

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result_details = {
                "saved_players": total_saved_players,
                "saved_entries": total_saved_entries,
                "saved_records": total_saved_records,
                "saved_line_predictions": total_saved_lines,
                "message": (
                    "MySQL Bulk save completed."
                    if overall_success
                    else "MySQL Bulk save completed with some errors."
                ),
            }

            if overall_success:
                self.logger.info(
                    f"Step3 Bulk Save 完了 (MySQL) ({duration:.2f}秒) - "
                    f"Saved Players: {total_saved_players}, "
                    f"Saved Entries: {total_saved_entries}, "
                    f"Saved Records: {total_saved_records}, "
                    f"Saved Lines: {total_saved_lines}"
                )
            else:
                self.logger.error(
                    f"Step3 Bulk Save (MySQL) 中にエラーが発生しました ({duration:.2f}秒)"
                )

            return overall_success, result_details

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.error(
                f"Step3 Bulk Save (MySQL) 全体で予期せぬエラーが発生しました ({duration:.2f}秒): {e}",
                exc_info=True,
            )
            return False, {"message": f"Unexpected error during MySQL bulk save: {e}"}
