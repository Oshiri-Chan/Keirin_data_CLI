"""
ステップ2: 開催詳細情報のデータセーバー (MySQL対応)
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# KeirinDataAccessorをインポート
from database.db_accessor import KeirinDataAccessor  # パスは環境に合わせてください


class Step2Saver:
    """
    ステップ2: 開催詳細情報を保存するクラス (MySQL対応)
    """

    def __init__(self, accessor: KeirinDataAccessor, logger: logging.Logger = None):
        """
        初期化

        Args:
            accessor (KeirinDataAccessor): データベースアクセサーインスタンス
            logger (logging.Logger, optional): ロガーオブジェクト。 Defaults to None.
        """
        self.accessor = accessor
        self.logger = logger or logging.getLogger(__name__)

    def _to_timestamp(self, datetime_str: Optional[str]) -> Optional[int]:
        """
        日時文字列 (YYYY-MM-DD HH:MM:SS or ISO形式) をUnixタイムスタンプ(秒)に変換。
        変換できない場合はNoneを返す。
        '0000-00-00 00:00:00' のような無効な日付もNoneとして扱う。
        """
        if not datetime_str or datetime_str == "0000-00-00 00:00:00":
            return None
        try:
            # ISO 8601形式 (YYYY-MM-DDTHH:MM:SSZ や YYYY-MM-DDTHH:MM:SS+00:00) の場合
            if "T" in datetime_str:
                dt_obj = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            else:  # YYYY-MM-DD HH:MM:SS 形式
                dt_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

            # タイムゾーン情報がない場合はUTCとみなす (API仕様に応じて調整)
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return int(dt_obj.timestamp())
        except ValueError:
            self.logger.warning(
                f"無効な日時形式でタイムスタンプ変換に失敗: {datetime_str}"
            )
            return None

    def _format_date(self, date_str: Any) -> Optional[str]:
        """日付文字列を YYYY-MM-DD 形式に変換、不正な場合はNoneを返す"""
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # YYYYMMDD 形式を試す
            if len(date_str) == 8 and date_str.isdigit():
                return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
            # YYYY-MM-DD HH:MM:SS (または YYYY-MM-DD) 形式を試す (時刻部分は無視)
            dt_obj = datetime.fromisoformat(
                date_str.split(" ")[0].replace("Z", "+00:00")
            )
            return dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            # YYYY-MM-DD 単独の形式も試す
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                self.logger.warning(f"不正な日付形式です: {date_str}")
            return None

    def save_schedules_batch(
        self, schedules_api_data: List[Dict[str, Any]], cup_id: str
    ):
        """
        複数のスケジュール情報をまとめて保存/更新 (MySQL用)

        Args:
            schedules_api_data (List[Dict[str, Any]]): APIから取得したスケジュール情報の辞書のリスト
            cup_id (str): 対象のカップID
        """
        self.logger.debug(
            f"save_schedules_batch 호출됨. cup_id: {cup_id}, schedules_api_data count: {len(schedules_api_data)}"
        )
        if schedules_api_data:
            self.logger.debug(
                f"First schedule data (sample): {schedules_api_data[0].get('id')}, date: {schedules_api_data[0].get('date')}"
            )

        if not schedules_api_data:
            self.logger.info(
                f"カップID {cup_id} の保存するスケジュールデータがありません。"
            )
            return

        to_save = []
        for schedule_data in schedules_api_data:
            schedule_id = str(schedule_data.get("id", ""))
            if not schedule_id:
                self.logger.warning(f"スケジュールIDが不足しています: {schedule_data}")
                continue

            formatted_date = self._format_date(schedule_data.get("date"))

            data = {
                "schedule_id": schedule_id,
                "cup_id": cup_id,
                "date": formatted_date,
                "day": (
                    int(schedule_data.get("day", 0))
                    if schedule_data.get("day") is not None
                    else None
                ),
                "schedule_index": (
                    int(schedule_data.get("index", 0))
                    if schedule_data.get("index") is not None
                    else None
                ),
                "entries_unfixed": 1 if schedule_data.get("entriesUnfixed") else 0,
            }
            to_save.append(data)

        if not to_save:
            self.logger.info(
                f"カップID {cup_id} の整形後、保存対象のスケジュールデータがありませんでした。"
            )
            return

        cols = [
            "schedule_id",
            "cup_id",
            "date",
            "day",
            "schedule_index",
            "entries_unfixed",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "schedule_id"
        ]
        update_sql = ", ".join(update_sql_parts)

        query = f"""
        INSERT INTO schedules ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = [
            tuple(data_dict.get(col) for col in cols) for data_dict in to_save
        ]

        try:
            self.accessor.execute_many(query, params_list)
            self.logger.info(
                f"カップID {cup_id}: {len(params_list)}件のスケジュール情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"カップID {cup_id} のスケジュール情報保存中にエラー: {e}",
                exc_info=True,
            )
            raise

    def save_races_batch(self, races_data: list[dict], cup_id: str):
        # races_data は _transform_race_data (updater内) で整形されたデータと想定
        race_ids_to_log = [r.get("race_id") for r in races_data[:5]]
        self.logger.info(
            f"save_races_batch に渡された races_data の race_ids (最初の{len(race_ids_to_log)}件): {race_ids_to_log} for cup_id {cup_id}"
        )

        if not races_data:
            self.logger.info(
                f"カップID {cup_id} の保存するレースデータがありません（入力が空）。"
            )
            return {
                "count": 0,
                "processed_race_ids": [],
                "error_details": "No race data provided",
            }

        params_list_races = []
        # successfully_prepared_race_ids は、実際に races テーブルへの保存パラメータが作成された race_id を追跡する
        successfully_prepared_race_ids = []

        self.logger.info(
            f"Cup ID {cup_id}: Preparing race data for batch insert. Extracted (race_id, schedule_id) pairs to be saved:"
        )
        log_count = 0

        for race_info in races_data:
            try:
                race_id = str(race_info["race_id"])  # 必須キー

                schedule_id_original = race_info.get("schedule_id")
                schedule_id_to_save = (
                    str(schedule_id_original)
                    if schedule_id_original is not None
                    else None
                )

                cup_id_to_save = str(race_info.get("cup_id", cup_id))

                race_number_val = race_info.get("number")
                race_number = (
                    int(race_number_val) if race_number_val is not None else None
                )

                race_class_name = (
                    str(race_info.get("class"))
                    if race_info.get("class") is not None
                    else None
                )
                race_type_val = race_info.get("race_type")
                race_type = (
                    str(race_type_val) if race_type_val is not None else None
                )  # 明示的にstr変換

                start_at = race_info.get("start_at")
                close_at = race_info.get("close_at")

                status_val = race_info.get("status")
                status = int(status_val) if status_val is not None else None

                cancel_val = race_info.get("cancel")
                cancel = (
                    cancel_val
                    if isinstance(cancel_val, bool)
                    else (
                        str(cancel_val).lower() == "true"
                        if cancel_val is not None
                        else False
                    )
                )

                cancel_reason = (
                    str(race_info.get("cancel_reason"))
                    if race_info.get("cancel_reason") is not None
                    else None
                )
                weather = (
                    str(race_info.get("weather"))
                    if race_info.get("weather") is not None
                    else None
                )

                wind_speed_val = race_info.get("wind_speed")
                # wind_speed は VARCHAR なので、float変換は不要。そのまま文字列で。
                wind_speed = str(wind_speed_val) if wind_speed_val is not None else None

                race_type3 = (
                    str(race_info.get("race_type3"))
                    if race_info.get("race_type3") is not None
                    else None
                )

                distance_val = race_info.get("distance")
                distance = int(distance_val) if distance_val is not None else None

                lap_val = race_info.get("lap")
                lap = int(lap_val) if lap_val is not None else None

                entries_number_val = race_info.get("entries_number")
                entries_number = (
                    int(entries_number_val) if entries_number_val is not None else None
                )

                is_grade_race_val = race_info.get("is_grade_race")
                is_grade_race = (
                    is_grade_race_val
                    if isinstance(is_grade_race_val, bool)
                    else (
                        str(is_grade_race_val).lower() == "true"
                        if is_grade_race_val is not None
                        else False
                    )
                )

                has_digest_video_val = race_info.get("has_digest_video")
                has_digest_video = (
                    has_digest_video_val
                    if isinstance(has_digest_video_val, bool)
                    else (
                        str(has_digest_video_val).lower() == "true"
                        if has_digest_video_val is not None
                        else False
                    )
                )

                digest_video = (
                    str(race_info.get("digest_video"))
                    if race_info.get("digest_video") is not None
                    else None
                )
                # digest_video_provider は VARCHAR(255) なので文字列でOK
                digest_video_provider = (
                    str(race_info.get("digest_video_provider"))
                    if race_info.get("digest_video_provider") is not None
                    else None
                )
                decided_at = race_info.get("decided_at")

                race_params = (
                    race_id,
                    cup_id_to_save,
                    schedule_id_to_save,
                    race_number,
                    race_class_name,
                    race_type,
                    start_at,
                    close_at,
                    status,
                    cancel,
                    cancel_reason,
                    weather,
                    wind_speed,
                    race_type3,
                    distance,
                    lap,
                    entries_number,
                    is_grade_race,
                    has_digest_video,
                    digest_video,
                    digest_video_provider,
                    decided_at,
                )
                params_list_races.append(race_params)
                successfully_prepared_race_ids.append(
                    race_id
                )  # パラメータ作成成功したIDを追加

                if log_count < 10:
                    self.logger.info(
                        f"  - Prepared for DB: race_id={race_id}, schedule_id={{schedule_id_to_save}} (original: {{schedule_id_original}}) for cup {{cup_id}}"
                    )
                    log_count += 1
                elif log_count == 10:
                    self.logger.info(
                        f"  - ... (logging for further race_id/schedule_id pairs in this batch suppressed for brevity preparazione per {cup_id})"
                    )
                    log_count += 1

            except KeyError as e:
                self.logger.error(
                    f"カップID {cup_id} のレースデータ処理中に必須キーエラー: {e}. Race info: {race_info}",
                    exc_info=True,
                )
                continue
            except ValueError as e:  # 主に int() 変換で発生
                self.logger.error(
                    f"カップID {cup_id} のレースデータ {race_info.get('race_id', 'N/A')} の型変換中にエラー: {e}. Race info: {race_info}",
                    exc_info=True,
                )
                continue
            except Exception as e:
                self.logger.error(
                    f"カップID {cup_id} のレースデータ {race_info.get('race_id', 'N/A')} の処理中に予期せぬエラー: {e}",
                    exc_info=True,
                )
                continue

        if not params_list_races:  # 実際に保存するパラメータリストが空の場合
            self.logger.info(
                f"カップID {cup_id} の整形後、保存対象のレースデータがありませんでした（エラーまたはフィルタリングによる）。"
            )
            # successfully_prepared_race_ids は try-catch で continue した場合もIDが入る可能性があるため、
            # params_list_races が空なら、実際にDBに渡されるものはない。
            return {
                "count": 0,
                "processed_race_ids": successfully_prepared_race_ids,
                "error_details": "No races to save after processing / filtering",
            }

        cols = [
            "race_id",
            "cup_id",
            "schedule_id",
            "number",
            "`class`",
            "race_type",
            "start_at",
            "close_at",
            "status",
            "cancel",
            "cancel_reason",
            "weather",
            "wind_speed",
            "race_type3",
            "distance",
            "lap",
            "entries_number",
            "is_grade_race",
            "has_digest_video",
            "digest_video",
            "digest_video_provider",
            "decided_at",
        ]

        num_expected_params = len(cols)
        if params_list_races and len(params_list_races[0]) != num_expected_params:
            msg = (
                f"カップID {cup_id}: Racesテーブルへのパラメータ数が一致しません。"
                f"期待する数: {num_expected_params}, 実際の数: {len(params_list_races[0])}."
                f"最初のパラメータセット: {params_list_races[0]}"
            )
            self.logger.error(msg)
            return {
                "count": 0,
                "processed_race_ids": successfully_prepared_race_ids,
                "error_details": "Races parameter count mismatch",
            }

        cols_sql = ", ".join(cols)
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"{col_name} = VALUES({col_name})"
            for col_name in cols
            if col_name != "race_id"
        ]
        update_sql = ", ".join(update_sql_parts)

        query_races = f"""
        INSERT INTO races ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        saved_race_count = 0
        try:
            self.accessor.execute_many(query_races, params_list_races)
            saved_race_count = len(params_list_races)
            self.logger.info(
                f"カップID {cup_id}: {saved_race_count} 件のレース情報をDBに保存/更新試行しました。"
            )

            if (
                successfully_prepared_race_ids
            ):  # race_status は保存試行した全IDに対して行う
                status_query = """
                INSERT INTO race_status (race_id)
                VALUES (%s)
                ON DUPLICATE KEY UPDATE race_id = VALUES(race_id)
                """
                # ここで使うIDは、実際にINSERT/UPDATEが試みられたID (エラーで中断された場合も含む)
                # ただし、racesテーブルへのFK制約で失敗する前にrace_statusを更新しても意味がないので、
                # やはりracesテーブルへの書き込みが成功したIDのみを対象とすべき。
                # execute_manyは部分的な成功を返さないため、エラーがなければ全件成功とみなす。
                # エラーがあれば、saved_race_count は0のままになる。

                # エラーがなければ、successfully_prepared_race_ids はすべて races テーブルに影響を与えたとみなせる
                status_params_list = [
                    (race_id,) for race_id in successfully_prepared_race_ids
                ]
                if status_params_list:  # リストが空でないことを確認
                    try:
                        self.accessor.execute_many(status_query, status_params_list)
                        self.logger.info(
                            f"カップID {cup_id}: {len(status_params_list)}件のrace_statusレコードを初期化/確認しました。"
                        )
                    except Exception as e_status:
                        self.logger.error(
                            f"カップID {cup_id} のrace_status初期化中にエラー: {e_status}",
                            exc_info=True,
                        )
                        raise e_status

            return {
                "count": saved_race_count,
                "processed_race_ids": successfully_prepared_race_ids,
                "error_details": None,
            }

        except Exception as e:
            self.logger.error(
                f"カップID {cup_id} のレース情報DB保存またはrace_status初期化中にエラー: {e}",
                exc_info=True,
            )
            # エラー発生時は保存件数0
            return {
                "count": 0,
                "processed_race_ids": successfully_prepared_race_ids,
                "error_details": str(e),
            }

    def _atomic_save_schedules(
        self, conn, cursor, schedules_api_data: List[Dict[str, Any]], cup_id: str
    ):
        """
        (Atomic) 複数のスケジュール情報をまとめて保存/更新 (MySQL用)
        """
        if not schedules_api_data:
            self.logger.info(
                f"(Atomic) カップID {cup_id} の保存するスケジュールデータがありません。"
            )
            return

        to_save = []
        for schedule_data in schedules_api_data:
            schedule_id = str(schedule_data.get("id", ""))
            if not schedule_id:
                self.logger.warning(
                    f"スケジュールIDが不足しています(atomic): {schedule_data}"
                )
                continue
            formatted_date = self._format_date(schedule_data.get("date"))
            data = {
                "schedule_id": schedule_id,
                "cup_id": cup_id,
                "date": formatted_date,
                "day": (
                    int(schedule_data.get("day", 0))
                    if schedule_data.get("day") is not None
                    else None
                ),
                "schedule_index": (
                    int(schedule_data.get("index", 0))
                    if schedule_data.get("index") is not None
                    else None
                ),
                "entries_unfixed": 1 if schedule_data.get("entriesUnfixed") else 0,
            }
            to_save.append(data)

        if not to_save:
            self.logger.info(
                f"(Atomic) カップID {cup_id} の整形後、保存対象のアトミックなスケジュールデータがありませんでした。"
            )
            return

        cols = [
            "schedule_id",
            "cup_id",
            "date",
            "day",
            "schedule_index",
            "entries_unfixed",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "schedule_id"
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"""
        INSERT INTO schedules ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = [
            tuple(data_dict.get(col) for col in cols) for data_dict in to_save
        ]
        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Atomic) カップID {cup_id}: {len(params_list)}件のスケジュール情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Atomic) カップID {cup_id} のスケジュール情報保存中にエラー: {e}",
                exc_info=True,
            )
            raise

    def _atomic_save_races(self, conn, cursor, races_data: list[dict], cup_id: str):
        """(Atomic) 複数のレース情報と関連情報を保存/更新 (MySQL用)"""
        if not races_data:
            self.logger.info(
                f"(Atomic) カップID {cup_id} の保存するアトミックなレースデータがありません。"
            )
            return {
                "count": 0,
                "processed_race_ids": [],
                "error_details": "No race data provided for atomic save",
            }

        params_list_races = []
        params_list_attributes = []
        params_list_url_sources = []
        successfully_prepared_race_ids: List[str] = []

        # races テーブルの列定義
        race_cols = [
            "race_id",
            "schedule_id",
            "cup_id",
            "day_index",
            "race_index",
            "race_name",
            "grade",
            "category",
            "distance",
            "race_type",
            "track_type",
            "wagering_deadline_timestamp",
            "start_datetime_timestamp",
            "first_day_flag",
            "status",
            "entries_count",
        ]
        race_cols_sql = ", ".join([f"`{col}`" for col in race_cols])
        race_values_sql = ", ".join(["%s"] * len(race_cols))
        race_update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in race_cols if col != "race_id"
        ]
        race_update_sql = ", ".join(race_update_sql_parts)
        query_races = f"""
        INSERT INTO races ({race_cols_sql})
        VALUES ({race_values_sql})
        ON DUPLICATE KEY UPDATE {race_update_sql}
        """

        # race_attributes テーブルの列定義
        attribute_cols = ["race_id", "attribute_name", "attribute_value"]
        attribute_cols_sql = ", ".join([f"`{col}`" for col in attribute_cols])
        attribute_values_sql = ", ".join(["%s"] * len(attribute_cols))
        attribute_update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in attribute_cols
            if col not in ["race_id", "attribute_name"]
        ]
        attribute_update_sql = ", ".join(attribute_update_sql_parts)
        query_attributes = f"""
        INSERT INTO race_attributes ({attribute_cols_sql})
        VALUES ({attribute_values_sql})
        ON DUPLICATE KEY UPDATE {attribute_update_sql}
        """

        # race_url_sources テーブルの列定義
        url_source_cols = ["race_id", "source_type", "url"]
        url_source_cols_sql = ", ".join([f"`{col}`" for col in url_source_cols])
        url_source_values_sql = ", ".join(["%s"] * len(url_source_cols))
        url_source_update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in url_source_cols
            if col not in ["race_id", "source_type"]
        ]
        url_source_update_sql = ", ".join(url_source_update_sql_parts)
        query_url_sources = f"""
        INSERT INTO race_url_sources ({url_source_cols_sql})
        VALUES ({url_source_values_sql})
        ON DUPLICATE KEY UPDATE {url_source_update_sql}
        """

        for race_info in races_data:
            race_id = str(race_info.get("race_id", ""))
            if not race_id:
                self.logger.warning(
                    f"(Atomic) カップID {cup_id}: race_id がないためスキップ: {race_info}"
                )
                continue

            # races テーブル用のパラメータ
            race_param = {col: race_info.get(col) for col in race_cols}
            race_param["cup_id"] = cup_id  # cup_id を強制的にセット
            # タイムスタンプ変換
            race_param["wagering_deadline_timestamp"] = self._to_timestamp(
                race_info.get("wagering_deadline_timestamp")
            )
            race_param["start_datetime_timestamp"] = self._to_timestamp(
                race_info.get("start_datetime_timestamp")
            )
            params_list_races.append(tuple(race_param.get(col) for col in race_cols))

            # race_attributes テーブル用のパラメータ
            attributes = race_info.get("attributes", {})
            if isinstance(attributes, dict):
                for attr_name, attr_value in attributes.items():
                    if attr_value is not None:  # 値がNoneでない場合のみ保存
                        params_list_attributes.append(
                            (race_id, str(attr_name), str(attr_value))
                        )

            # race_url_sources テーブル用のパラメータ
            url_sources = race_info.get("url_sources", {})
            if isinstance(url_sources, dict):
                for source_type, url in url_sources.items():
                    if url:
                        params_list_url_sources.append(
                            (race_id, str(source_type), str(url))
                        )

            successfully_prepared_race_ids.append(race_id)

        if params_list_races:
            try:
                cursor.executemany(query_races, params_list_races)
                self.logger.info(
                    f"(Atomic) カップID {cup_id}: {len(params_list_races)}件のレース基本情報を保存/更新しました。"
                )
            except Exception as e:
                self.logger.error(
                    f"(Atomic) カップID {cup_id} のレース基本情報保存中にエラー: {e}",
                    exc_info=True,
                )
                raise
        else:
            self.logger.info(
                f"(Atomic) カップID {cup_id}: 整形後、保存対象のレース基本情報がありませんでした。"
            )

        if params_list_attributes:
            try:
                cursor.executemany(query_attributes, params_list_attributes)
                self.logger.info(
                    f"(Atomic) カップID {cup_id}: {len(params_list_attributes)}件のレース属性情報を保存/更新しました。"
                )
            except Exception as e:
                self.logger.error(
                    f"(Atomic) カップID {cup_id} のレース属性情報保存中にエラー: {e}",
                    exc_info=True,
                )
                raise
        else:
            self.logger.info(
                f"(Atomic) カップID {cup_id}: 保存対象のレース属性情報がありませんでした。"
            )

        if params_list_url_sources:
            try:
                cursor.executemany(query_url_sources, params_list_url_sources)
                self.logger.info(
                    f"(Atomic) カップID {cup_id}: {len(params_list_url_sources)}件のレースURLソースを保存/更新しました。"
                )
            except Exception as e:
                self.logger.error(
                    f"(Atomic) カップID {cup_id} のレースURLソース保存中にエラー: {e}",
                    exc_info=True,
                )
                raise
        else:
            self.logger.info(
                f"(Atomic) カップID {cup_id}: 保存対象のレースURLソースがありませんでした。"
            )

    def _atomic_save_cup_attributes(
        self, conn, cursor, cup_id: str, attributes_data: List[Dict[str, Any]]
    ):
        """(Atomic) カップの属性情報を保存/更新 (MySQL用)"""
        if not attributes_data:
            self.logger.info(
                f"(Atomic) カップID {cup_id} の保存するアトミックなカップ属性データがありません。"
            )
            return

        params_list = []
        for attr in attributes_data:
            params_list.append((cup_id, attr.get("type"), attr.get("name")))

        if not params_list:
            self.logger.info(
                f"(Atomic) カップID {cup_id}: 整形後、保存対象のアトミックなカップ属性データがありませんでした。"
            )
            return

        query = """
        INSERT INTO cup_attributes (cup_id, type, name)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE name = VALUES(name) -- typeもキーなら更新対象に含めるか検討
        """
        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Atomic) カップID {cup_id}: {len(params_list)}件のカップ属性情報をアトミックに保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Atomic) カップID {cup_id} のカップ属性情報保存中にエラー: {e}",
                exc_info=True,
            )
            raise

    def _atomic_save_race_statuses(self, conn, cursor, race_ids: List[str]):
        """(Atomic) 指定されたレースIDの race_status レコードを作成/更新 (MySQL用)"""
        if not race_ids:
            self.logger.info(
                "保存するアトミックな race_status データがありません (race_idsが空)。"
            )
            return

        # 初期ステータスを設定 (例: pending)
        # race_status テーブルのスキーマに合わせて調整してください。
        # ここでは race_id のみ INSERT し、他のカラムはデフォルト値またはNULLを想定。
        # ON DUPLICATE KEY UPDATE は、既に存在する race_id に対しては何もしない (IGNOREに近い挙動)か、
        # あるいは特定のカラムを更新するかを定義します。ここでは IGNORE 的な動作を狙い、
        # race_id が存在すれば何もしないように、更新対象のフィールドを指定しません。
        # もし特定の更新が必要な場合は、`stepN_status = VALUES(stepN_status)` のように追加します。
        query = """
        INSERT INTO race_status (race_id)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE race_id = VALUES(race_id) -- 既に存在する場合は何もしない (実質IGNORE)
        """
        # パラメータリストは (race_id,) のタプルのリスト
        params_list = [(race_id,) for race_id in race_ids]

        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Atomic) {len(params_list)}件のレースステータスレコードを初期化/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Atomic) レースステータスレコードの初期化/更新中にエラー: {e}",
                exc_info=True,
            )
            raise

    def _atomic_save_cup_details(
        self, conn, cup_id: str, cup_detail_data: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        カップ詳細情報（スケジュール、レース、属性、ステータス）をトランザクション内でアトミックに保存する内部メソッド。
        Args:
            conn: データベース接続オブジェクト
            cup_id (str): カップID
            cup_detail_data (Dict[str, Any]): APIから取得したカップ詳細データ
        Returns:
            Tuple[bool, List[str]]: 成功したかどうか、処理されたレースIDのリスト
        """
        processed_race_ids_in_tx: List[str] = []
        cursor = None

        try:
            cursor = conn.cursor(dictionary=True)

            schedules_api = cup_detail_data.get("schedules", [])
            if schedules_api:
                self.logger.info(
                    f"(TX) カップID {cup_id}: スケジュール情報を保存開始。"
                )
                self._atomic_save_schedules(conn, cursor, schedules_api, cup_id)
            else:
                self.logger.info(f"(TX) カップID {cup_id}: スケジュール情報なし。")

            races_api = cup_detail_data.get("races", [])
            if races_api:
                self.logger.info(f"(TX) カップID {cup_id}: レース情報を保存開始。")
                self._atomic_save_races(conn, cursor, races_api, cup_id)
                processed_race_ids_in_tx.extend(
                    [str(r.get("race_id")) for r in races_api if r.get("race_id")]
                )
            else:
                self.logger.info(f"(TX) カップID {cup_id}: レース情報なし。")

            attributes_api = cup_detail_data.get("attributes", [])
            if attributes_api:
                self.logger.info(f"(TX) カップID {cup_id}: カップ属性情報を保存開始。")
                self._atomic_save_cup_attributes(conn, cursor, cup_id, attributes_api)
            else:
                self.logger.info(f"(TX) カップID {cup_id}: カップ属性情報なし。")

            if processed_race_ids_in_tx:
                self.logger.info(
                    f"(TX) カップID {cup_id}: レースステータスを初期化開始。対象レース数: {len(processed_race_ids_in_tx)}"
                )
                self._atomic_save_race_statuses(conn, cursor, processed_race_ids_in_tx)
            else:
                self.logger.info(
                    f"(TX) カップID {cup_id}: 初期化対象のレースステータスなし。"
                )

            self.logger.info(
                f"(TX) カップID {cup_id}: 全詳細情報のアトミック保存成功。"
            )
            return True, processed_race_ids_in_tx

        except Exception as e_tx:
            self.logger.error(
                f"(TX) カップID {cup_id} のアトミック保存中にエラー: {e_tx}",
                exc_info=True,
            )
            # トランザクションは呼び出し元でロールバックされるので、ここではFalseとIDリストを返す
            return False, processed_race_ids_in_tx  # エラー発生時も処理済みIDを返す
        finally:
            if cursor:
                cursor.close()

    def save_cup_details(
        self, cup_id: str, cup_detail_data: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """
        カップ詳細情報をデータベースに保存します (トランザクション対応)。
        スケジュール情報、レース情報、カップ属性情報などをまとめてアトミックに処理します。

        Args:
            cup_id (str): 対象のカップID
            cup_detail_data (Dict[str, Any]): APIから取得したカップ詳細情報
                期待する主なキー: "cup" (schedules, attributesを含む), "races"

        Returns:
            Tuple[bool, List[str]]: (成功したかどうか, 保存/更新されたレースIDのリスト)
        """
        self.logger.info(
            f"カップID {cup_id} の詳細情報の保存処理を開始します（トランザクション対応）。"
        )
        try:
            # トランザクション内で _atomic_save_cup_details を実行
            success, processed_race_ids = self.accessor.execute_in_transaction(
                self._atomic_save_cup_details,
                cup_id,
                cup_detail_data,
            )
            if success:
                self.logger.info(
                    f"カップID {cup_id}: 詳細情報のアトミックな保存に成功しました。処理レース数: {len(processed_race_ids)}"
                )
            else:
                # execute_in_transaction が False を返すのは通常、内部でキャッチされなかった例外がない場合か、
                # _atomic_save_cup_details が明示的に False を返した場合（今回はそのようなロジックはない）。
                # 基本的には例外が発生し、下の except ブロックで捕捉される想定。
                self.logger.warning(
                    f"カップID {cup_id}: 詳細情報のアトミックな保存処理が完了しましたが、結果は「失敗」でした。ログを確認してください。"
                )
            return success, processed_race_ids
        except Exception as e:
            # execute_in_transaction 内で発生した例外がここでキャッチされる
            self.logger.error(
                f"カップID {cup_id} の詳細情報の保存トランザクション全体でエラーが発生しました: {e}",
                exc_info=True,
            )
            return False, []  # 失敗を示すフラグと空のリストを返す

    def update_race_step2_status_batch(self, race_ids: List[str], status: str) -> None:
        """
        指定されたレースIDリストの race_status.step2_status を更新する (MySQL用)
        更新前にFOR UPDATEでロックを取得する。
        """
        if not race_ids:
            self.logger.info("更新対象のレースIDがありません (Step2ステータス)。")
            return

        # トランザクション内でロック取得と更新を行う関数
        def _update_status_in_transaction(conn):
            cursor = None
            updated_count = 0
            try:
                cursor = conn.cursor(dictionary=True)
                for race_id in race_ids:
                    # 1. FOR UPDATE で行をロックしつつ現在のステータス等を確認 (任意)
                    #    実際には race_status が存在するかどうかを確認するだけでも良いかもしれない
                    lock_query = "SELECT race_id, step2_status FROM race_status WHERE race_id = %s FOR UPDATE"
                    cursor.execute(lock_query, (race_id,))
                    locked_row = cursor.fetchone()

                    if locked_row:
                        self.logger.debug(
                            f"Race ID {race_id} をロックしました。現在のstep2_status: {locked_row.get('step2_status')}"
                        )
                        # 2. ステータスを更新
                        update_query = """
                        UPDATE race_status
                        SET step2_status = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE race_id = %s
                        """
                        valid_status = status[:10]  # VARCHAR(10)に合わせて
                        cursor.execute(update_query, (valid_status, race_id))
                        if cursor.rowcount > 0:
                            updated_count += 1
                            self.logger.info(
                                f"Race ID {race_id} のStep2ステータスを '{valid_status}' に更新しました。"
                            )
                        else:
                            self.logger.warning(
                                f"Race ID {race_id} のStep2ステータス更新に失敗 (行影響なし)。"
                            )
                    else:
                        self.logger.warning(
                            f"Race ID {race_id} はrace_statusテーブルに存在しないか、ロックできませんでした。"
                        )
                return updated_count  # 更新件数を返す (任意)
            finally:
                if cursor:
                    cursor.close()

        try:
            # self.accessor.execute_in_transaction を使って上記の関数を実行
            num_updated = self.accessor.execute_in_transaction(
                _update_status_in_transaction
            )
            self.logger.info(
                f"合計 {num_updated}/{len(race_ids)} 件のレースのStep2ステータス更新処理が完了しました。"
            )

        except Exception as e:
            self.logger.error(
                f"レースStep2ステータス更新トランザクション中にエラー (IDs: {race_ids}, Status: {status}): {e}",
                exc_info=True,
            )
            raise

    # 既存の get_cup_data は accessor を使う形にリファクタリングするか、
    # 不要であれば削除します。
    # def get_cup_data(self, cup_id: str) -> Optional[Dict[str, Any]]:
    #     query = "SELECT * FROM cups WHERE cup_id = %s"
    #     try:
    #         result = self.accessor.execute_query(query, (cup_id,), fetch_one=True)
    #         if result:
    #             self.logger.info(f"開催ID {cup_id} の情報をDBから取得しました。")
    #             return result
    #         else:
    #             self.logger.warning(f"開催ID {cup_id} の情報がDBに見つかりませんでした。")
    #             return None
    #     except Exception as e:
    #         self.logger.error(f"開催情報取得中(ID: {cup_id}): {e}", exc_info=True)
    #         return None


# 以前の bulk_save_step2_data はMySQL非対応であり、
# 新しい save_cup_details とその中のバッチメソッドに役割が移譲されたため削除します。
