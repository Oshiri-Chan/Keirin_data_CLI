"""
ステップ3: レース詳細情報 (選手、出走表、選手成績、ライン) のデータセーバー (MySQL対応)
"""

import inspect  # Add import for inspect
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from database.db_accessor import KeirinDataAccessor


class Step3Saver:
    """
    ステップ3: レース詳細情報を保存するクラス (MySQL対応)
    """

    def __init__(self, accessor: KeirinDataAccessor, logger: logging.Logger = None):
        self.accessor = accessor
        self.logger = logger or logging.getLogger(__name__)

    def _get_existing_region_ids(self) -> set[str]:
        """regions テーブルに存在する region_id のセットを取得する"""
        query = "SELECT region_id FROM regions"
        existing_ids = set()  # デフォルトは空セット
        try:
            result = self.accessor.execute_query(query)
            if result:
                # ★★★ ログを return 前に移動 ★★★
                existing_ids = {
                    str(row["region_id"]) for row in result
                }  # 文字列に変換しておく
                self.logger.debug(
                    f"_get_existing_region_ids: 取得成功 - Result: {result}, Parsed IDs: {existing_ids}"
                )
            else:
                # ★★★ 結果が空だった場合のログを追加 ★★★
                self.logger.warning(
                    "_get_existing_region_ids: クエリは成功しましたが、regions テーブルからデータが返されませんでした。"
                )
            return existing_ids
        except Exception as e:
            # self.logger.error(f"既存の region_id の取得に失敗しました: {e}", exc_info=True) # 元のログ
            self.logger.error(
                f"既存の region_id の取得中に例外発生: {e}", exc_info=True
            )  # 修正後のログ
            return set()  # エラー時は空セットを返す

    def _to_timestamp(self, datetime_str: Optional[str]) -> Optional[int]:
        if not datetime_str or datetime_str == "0000-00-00 00:00:00":
            return None
        try:
            if "T" in datetime_str:  # ISO 8601形式
                dt_obj = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            else:  # YYYY-MM-DD HH:MM:SS 形式
                dt_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

            if dt_obj.tzinfo is None:  # JSTなどの指定がない場合はUTCとみなす
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return int(dt_obj.timestamp())
        except ValueError:
            self.logger.warning(
                f"無効な日時形式でタイムスタンプ変換に失敗: {datetime_str}"
            )
            return None

    def _format_date(self, date_str: Any) -> Optional[str]:
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            if len(date_str) == 8 and date_str.isdigit():  # YYYYMMDD
                return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
            dt_obj = datetime.fromisoformat(
                date_str.split(" ")[0].replace("Z", "+00:00")
            )  # YYYY-MM-DD HH:MM:SS or YYYY-MM-DD
            return dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            try:  # YYYY-MM-DD
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str  # 元の形式が既に YYYY-MM-DD
            except ValueError:
                self.logger.warning(f"不正な日付形式です: {date_str}")
                return None

    def _convert_birth_date(self, birth_date_str):
        """
        生年月日文字列をMySQL用の日付形式に変換
        """
        if not birth_date_str or not isinstance(birth_date_str, str):
            return None

        try:
            if len(birth_date_str) == 8 and birth_date_str.isdigit():
                year = birth_date_str[:4]
                month = birth_date_str[4:6]
                day = birth_date_str[6:8]
                return f"{year}-{month}-{day}"
        except Exception as e:
            self.logger.warning(
                f"生年月日の変換に失敗しました: {birth_date_str}, エラー: {e}"
            )

        return None

    def _convert_birthday_to_date(self, birthday_str: str) -> Optional[str]:
        """
        生年月日文字列をSQLite用の日付形式に変換

        Args:
            birthday_str: YYYYMMDD形式の生年月日文字列

        Returns:
            YYYY-MM-DD形式の日付文字列、またはNone
        """
        if not birthday_str or not isinstance(birthday_str, str):
            return None

        try:
            if len(birthday_str) == 8 and birthday_str.isdigit():
                year = birthday_str[:4]
                month = birthday_str[4:6]
                day = birthday_str[6:8]
                return f"{year}-{month}-{day}"
        except Exception as e:
            self.logger.warning(
                f"生年月日の変換に失敗しました: {birthday_str}, エラー: {e}"
            )

        return None

    def _convert_gender_to_int(self, gender):
        """
        性別をMySQL用の整数値に変換
        """
        if isinstance(gender, str):
            if gender == "男":
                return 1
            elif gender == "女":
                return 2
            else:
                return 0
        elif isinstance(gender, int):
            if gender in (1, 2):
                return gender
            else:
                return 0
        else:
            return 0

    def save_players_batch(
        self,
        players_data_list: List[Dict[str, Any]],
        race_id: str,
        batch_size: int,
    ) -> bool:
        """
        選手データを一括保存する（MySQL用バッチ処理）

        Args:
            players_data_list: 選手データのリスト
            race_id: レースID
            batch_size: バッチサイズ

        Returns:
            bool: 保存が成功した場合True、失敗した場合False
        """
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)}) before executing {inspect.currentframe().f_code.co_name} for race_id {race_id}"
            )
            raise TypeError(
                f"Accessor is not a KeirinDataAccessor instance in {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} (type: {type(self.accessor)})."
            )

        self.logger.info(
            f"選手データの一括保存を開始します。データ数: {len(players_data_list)}, レースID: {race_id}, バッチサイズ: {batch_size}"
        )

        if not players_data_list:
            self.logger.warning(f"保存する選手データがありません。レースID: {race_id}")
            return False

        # MySQLのplayersテーブル構造に合わせる（PRIMARY KEY: race_id, player_id）
        cols = [
            "race_id",
            "player_id",
            "name",
            "class",
            "player_group",
            "prefecture",
            "term",
            "region_id",
            "yomi",
            "birthday",
            "age",
            "gender",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in cols
            if col not in ["race_id", "player_id"]
        ]
        update_sql = ", ".join(update_sql_parts)

        # MySQL用の INSERT ... ON DUPLICATE KEY UPDATE構文
        query = f"""
        INSERT INTO players ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        processed_data_count_total = 0
        skipped_count_total = 0
        all_batches_successful = True

        for i in range(0, len(players_data_list), batch_size):
            batch = players_data_list[i : i + batch_size]
            batch_values = []

            for player_data in batch:
                # player_idの検証（Step3Updaterは"player_id"キーを使用）
                player_id = player_data.get("player_id")  # "id" -> "player_id"に変更
                if not player_id:
                    self.logger.warning(
                        f"選手データにplayer_idなし。スキップ: {player_data}, レースID: {race_id}"
                    )
                    skipped_count_total += 1
                    continue

                # MySQL用のplayersテーブル構造に合わせてデータを変換
                processed_data = [
                    race_id,  # race_id
                    player_id,  # player_id
                    player_data.get("name"),
                    player_data.get("class"),
                    player_data.get(
                        "player_group"
                    ),  # Step3Updaterは"player_group"キーを使用
                    player_data.get("prefecture"),
                    player_data.get("term"),
                    player_data.get("region_id"),  # Step3Updaterは"region_id"キーを使用
                    player_data.get("yomi"),
                    self._convert_birth_date(player_data.get("birthday")),
                    player_data.get("age"),
                    self._convert_gender_to_int(player_data.get("gender")),
                ]
                batch_values.append(processed_data)

            # バッチが空の場合はスキップ
            if not batch_values:
                self.logger.warning(
                    f"選手データ バッチ {i//batch_size + 1} は全てスキップされました。レースID: {race_id}"
                )
                continue

            try:
                affected_rows = self.accessor.execute_many(query, batch_values)
                processed_data_count_total += len(batch_values)
                self.logger.debug(
                    f"選手データ バッチ {i//batch_size + 1} を保存しました。処理件数: {len(batch_values)}, 影響行数: {affected_rows}, レースID: {race_id}"
                )
            except Exception as e:
                self.logger.error(
                    f"選手データ バッチ {i//batch_size + 1} の保存に失敗しました。レースID: {race_id}, エラー: {str(e)}",
                    exc_info=True,
                )
                skipped_count_total += len(batch_values)
                all_batches_successful = False

        self.logger.info(
            f"選手データの一括保存が完了しました。レースID: {race_id}, 処理件数: {processed_data_count_total}, スキップ件数: {skipped_count_total}, 成功: {all_batches_successful}"
        )
        return all_batches_successful

    def save_entries_batch(
        self,
        entries_data_list: List[Dict[str, Any]],
        race_id: str,
        batch_size: int,
    ) -> bool:
        """
        出走データを一括保存する（MySQL用バッチ処理）

        Args:
            entries_data_list: 出走データのリスト
            race_id: レースID
            batch_size: バッチサイズ

        Returns:
            bool: 保存が成功した場合True、失敗した場合False
        """
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)}) before executing {inspect.currentframe().f_code.co_name} for race_id {race_id}"
            )
            raise TypeError(
                f"Accessor is not a KeirinDataAccessor instance in {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} (type: {type(self.accessor)})."
            )

        self.logger.info(
            f"出走データの一括保存を開始します。データ数: {len(entries_data_list)}, レースID: {race_id}, バッチサイズ: {batch_size}"
        )

        if not entries_data_list:
            self.logger.warning(f"保存する出走データがありません。レースID: {race_id}")
            return False

        # MySQLのentriesテーブル構造に合わせる（PRIMARY KEY: race_id, number）
        cols = [
            "number",
            "race_id",
            "absent",
            "player_id",
            "bracket_number",
            "player_current_term_class",
            "player_current_term_group",
            "player_previous_term_class",
            "player_previous_term_group",
            "has_previous_class_group",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in cols
            if col not in ["race_id", "number"]
        ]
        update_sql = ", ".join(update_sql_parts)

        # MySQL用の INSERT ... ON DUPLICATE KEY UPDATE構文
        query = f"""
        INSERT INTO entries ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        processed_data_count_total = 0
        skipped_count_total = 0
        all_batches_successful = True

        for i in range(0, len(entries_data_list), batch_size):
            batch = entries_data_list[i : i + batch_size]
            batch_values = []

            for entry_data in batch:
                # MySQLのentriesテーブル構造に合わせてデータを変換
                processed_data = [
                    entry_data.get("number"),  # number (車番)
                    race_id,  # race_id
                    1 if entry_data.get("absent") else 0,  # absent
                    entry_data.get("player_id"),  # Step3Updaterは"player_id"キーを使用
                    entry_data.get(
                        "bracket_number"
                    ),  # Step3Updaterは"bracket_number"キーを使用
                    entry_data.get(
                        "player_current_term_class"
                    ),  # Step3Updaterは"player_current_term_class"キーを使用
                    entry_data.get(
                        "player_current_term_group"
                    ),  # Step3Updaterは"player_current_term_group"キーを使用
                    entry_data.get(
                        "player_previous_term_class"
                    ),  # Step3Updaterは"player_previous_term_class"キーを使用
                    entry_data.get(
                        "player_previous_term_group"
                    ),  # Step3Updaterは"player_previous_term_group"キーを使用
                    (
                        1 if entry_data.get("has_previous_class_group") else 0
                    ),  # Step3Updaterは"has_previous_class_group"キーを使用
                ]
                batch_values.append(processed_data)

            try:
                affected_rows = self.accessor.execute_many(query, batch_values)
                processed_data_count_total += len(batch_values)
                self.logger.debug(
                    f"出走データ バッチ {i//batch_size + 1} を保存しました。処理件数: {len(batch_values)}, 影響行数: {affected_rows}, レースID: {race_id}"
                )
            except Exception as e:
                self.logger.error(
                    f"出走データ バッチ {i//batch_size + 1} の保存に失敗しました。レースID: {race_id}, エラー: {str(e)}",
                    exc_info=True,
                )
                skipped_count_total += len(batch_values)
                all_batches_successful = False

        self.logger.info(
            f"出走データの一括保存が完了しました。レースID: {race_id}, 処理件数: {processed_data_count_total}, スキップ件数: {skipped_count_total}, 成功: {all_batches_successful}"
        )
        return all_batches_successful

    def save_player_records_batch(
        self,
        player_records_data_list: List[Dict[str, Any]],
        race_id: str,
        batch_size: int,
    ) -> bool:
        """
        選手成績データを一括保存する（MySQL用バッチ処理）

        Args:
            player_records_data_list: 選手成績データのリスト
            race_id: レースID
            batch_size: バッチサイズ

        Returns:
            bool: 保存が成功した場合True、失敗した場合False
        """
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)}) before executing {inspect.currentframe().f_code.co_name} for race_id {race_id}"
            )
            raise TypeError(
                f"Accessor is not a KeirinDataAccessor instance in {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} (type: {type(self.accessor)})."
            )

        self.logger.info(
            f"選手成績データの一括保存を開始します。データ数: {len(player_records_data_list)}, レースID: {race_id}, バッチサイズ: {batch_size}"
        )

        if not player_records_data_list:
            self.logger.warning(
                f"保存する選手成績データがありません。レースID: {race_id}"
            )
            return False

        # MySQLのplayer_recordsテーブル構造に合わせる（PRIMARY KEY: race_id, player_id）
        cols = [
            "race_id",
            "player_id",
            "gear_ratio",
            "style",
            "race_point",
            "comment",
            "prediction_mark",
            "first_rate",
            "second_rate",
            "third_rate",
            "has_modified_gear_ratio",
            "modified_gear_ratio",
            "modified_gear_ratio_str",
            "gear_ratio_str",
            "race_point_str",
            "previous_cup_id",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in cols
            if col not in ["race_id", "player_id"]
        ]
        update_sql = ", ".join(update_sql_parts)

        # MySQL用の INSERT ... ON DUPLICATE KEY UPDATE構文
        query = f"""
        INSERT INTO player_records ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        processed_data_count_total = 0
        skipped_count_total = 0
        all_batches_successful = True

        for i in range(0, len(player_records_data_list), batch_size):
            batch = player_records_data_list[i : i + batch_size]
            batch_values = []

            for record_data in batch:
                # player_idの検証
                player_id = record_data.get("player_id")
                if not player_id:
                    self.logger.warning(
                        f"選手成績データにplayer_idなし。スキップ: {record_data}, レースID: {race_id}"
                    )
                    skipped_count_total += 1
                    continue

                # MySQLのplayer_recordsテーブル構造に合わせてデータを変換
                processed_data = [
                    race_id,  # race_id
                    player_id,  # player_id（Step3Updaterで整形されたキー）
                    self._safe_float_convert(
                        record_data.get("gear_ratio")
                    ),  # gear_ratio
                    record_data.get("style"),  # style
                    self._safe_float_convert(
                        record_data.get("race_point")
                    ),  # race_point
                    record_data.get("comment"),  # comment
                    self._safe_int_convert(
                        record_data.get("prediction_mark")
                    ),  # prediction_mark
                    self._safe_float_convert(
                        record_data.get("first_rate")
                    ),  # first_rate
                    self._safe_float_convert(
                        record_data.get("second_rate")
                    ),  # second_rate
                    self._safe_float_convert(
                        record_data.get("third_rate")
                    ),  # third_rate
                    (
                        1 if record_data.get("has_modified_gear_ratio") else 0
                    ),  # has_modified_gear_ratio
                    self._safe_float_convert(
                        record_data.get("modified_gear_ratio")
                    ),  # modified_gear_ratio
                    record_data.get(
                        "modified_gear_ratio_str"
                    ),  # modified_gear_ratio_str
                    record_data.get("gear_ratio_str"),  # gear_ratio_str
                    record_data.get("race_point_str"),  # race_point_str
                    record_data.get("previous_cup_id"),  # previous_cup_id
                ]
                batch_values.append(processed_data)

            # バッチが空の場合はスキップ
            if not batch_values:
                self.logger.warning(
                    f"選手成績データ バッチ {i//batch_size + 1} は全てスキップされました。レースID: {race_id}"
                )
                continue

            try:
                affected_rows = self.accessor.execute_many(query, batch_values)
                processed_data_count_total += len(batch_values)
                self.logger.debug(
                    f"選手成績データ バッチ {i//batch_size + 1} を保存しました。処理件数: {len(batch_values)}, 影響行数: {affected_rows}, レースID: {race_id}"
                )
            except Exception as e:
                self.logger.error(
                    f"選手成績データ バッチ {i//batch_size + 1} の保存に失敗しました。レースID: {race_id}, エラー: {str(e)}",
                    exc_info=True,
                )
                skipped_count_total += len(batch_values)
                all_batches_successful = False

        self.logger.info(
            f"選手成績データの一括保存が完了しました。レースID: {race_id}, 処理件数: {processed_data_count_total}, スキップ件数: {skipped_count_total}, 成功: {all_batches_successful}"
        )
        return all_batches_successful

    def _safe_float_convert(self, value: Any) -> Optional[float]:
        """安全にfloat変換を行うヘルパーメソッド"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int_convert(self, value: Any) -> Optional[int]:
        """安全にint変換を行うヘルパーメソッド"""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def save_line_predictions_batch(
        self, line_prediction_data: Optional[Dict[str, Any]], race_id: str
    ):
        """
        特定レースのライン予想情報を保存/更新 (MySQL用)
        Updaterから整形済みのデータを受け取る
        """
        if self.accessor is None:
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is None before executing {inspect.currentframe().f_code.co_name} for race_id {race_id}"
            )
            raise AttributeError(
                f"Accessor is None in {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}, cannot proceed."
            )
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)}) before executing {inspect.currentframe().f_code.co_name} for race_id {race_id}"
            )
            raise TypeError(
                f"Accessor is not a KeirinDataAccessor instance in {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} (type: {type(self.accessor)})."
            )
        self.logger.debug(
            f"Step3Saver ({self.__class__.__name__}): accessor type is {type(self.accessor)} in {inspect.currentframe().f_code.co_name} for race_id {race_id}"
        )

        if line_prediction_data is None:
            self.logger.info(f"レースID {race_id} のライン予想データがありません。")
            return

        if not isinstance(line_prediction_data, dict):
            self.logger.warning(
                f"レースID {race_id} のライン予想データが予期せぬ型です: {type(line_prediction_data)}"
            )
            return

        # Updaterから整形済みのデータを受け取る
        line_type = str(line_prediction_data.get("lineType", ""))
        line_formation = str(line_prediction_data.get("lineFormation", ""))

        if not line_type and not line_formation:
            self.logger.info(
                f"レースID {race_id}: line_type と line_formation が両方空のため、ライン情報を保存しません。"
            )
            return

        to_save = {
            "race_id": race_id,
            "line_type": line_type,
            "line_formation": line_formation,
        }
        cols = ["race_id", "line_type", "line_formation"]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "race_id"
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"""
        INSERT INTO line_predictions ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = [tuple(to_save.get(col) for col in cols)]

        try:
            self.accessor.execute_many(query, params_list)
            self.logger.info(f"レースID {race_id}: ライン予想情報を保存/更新しました。")
        except Exception as e:
            self.logger.error(
                f"レースID {race_id} のライン予想情報保存中にエラー: {e}",
                exc_info=True,
            )
            raise

    def save_race_details_step3(
        self,
        race_id: str,
        players_data: List[Dict[str, Any]],
        entries_data: List[Dict[str, Any]],
        player_records_data: List[
            Dict[str, Any]
        ],  # APIでは 'records' や 'playerRaceResults' などのキー
        line_prediction_data: Optional[Dict[str, Any]],  # APIでは 'linePrediction'
        batch_size: int,  # batch_size を追加
    ) -> bool:
        """
        レース詳細情報 (選手、出走表、選手成績、ライン等) をまとめて保存 (MySQL用)
        トランザクション管理を使用して一貫性を保証する。
        """
        if self.accessor is None:
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is None before executing {inspect.currentframe().f_code.co_name} for race_id {race_id}"
            )
            raise AttributeError(
                f"Accessor is None in {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}, cannot proceed."
            )
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)}) before executing {inspect.currentframe().f_code.co_name} for race_id {race_id}"
            )
            raise TypeError(
                f"Accessor is not a KeirinDataAccessor instance in {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} (type: {type(self.accessor)})."
            )
        self.logger.debug(
            f"Step3Saver ({self.__class__.__name__}): accessor type is {type(self.accessor)} in {inspect.currentframe().f_code.co_name} for race_id {race_id}"
        )

        def _save_in_transaction(conn):
            """トランザクション内で実行される保存処理"""
            all_components_successful = True
            cursor = None
            try:
                cursor = conn.cursor(dictionary=True)
                self.logger.info(
                    f"レースID {race_id}: Step3データの保存を開始します（トランザクション内）。"
                )

                # 保存対象の処理と対応するテーブル名(deadrock.iniのキーと一致させる)
                # 注意: players, entries, player_records は実際には race_id を外部キーとして持つので、
                #       races テーブルや race_status テーブルの更新が先に行われるべきケースも考慮する。
                #       ここでは Step3Saver が直接扱うテーブルのみをリストアップ。
                save_operations = {
                    # 'テーブル名(deadrock.iniと一致)': (実行条件, 保存メソッド呼び出しラムダ)
                    "players": (
                        players_data,
                        lambda: self._save_players_batch_with_cursor(
                            players_data, race_id, batch_size, conn, cursor
                        ),
                    ),
                    "entries": (
                        entries_data,
                        lambda: self._save_entries_batch_with_cursor(
                            entries_data, race_id, batch_size, conn, cursor
                        ),
                    ),
                    "player_records": (
                        player_records_data,
                        lambda: self._save_player_records_batch_with_cursor(
                            player_records_data, race_id, batch_size, conn, cursor
                        ),
                    ),
                    "line_predictions": (
                        line_prediction_data is not None,
                        lambda: self._save_line_predictions_batch_with_cursor(
                            line_prediction_data, race_id, conn, cursor
                        ),
                    ),
                }

                executed_operations = set()

                if self.accessor.lock_order:
                    self.logger.debug(
                        f"定義されたロック順序 ({self.accessor.lock_order}) に従って処理します。"
                    )
                    for table_name_in_order in self.accessor.lock_order:
                        if table_name_in_order in save_operations:
                            condition, operation_func = save_operations[
                                table_name_in_order
                            ]
                            if condition:  # 実行条件がTrueの場合のみ実行
                                self.logger.debug(
                                    f"ロック順序に基づき {table_name_in_order} の保存処理を実行します。"
                                )
                                if not operation_func():
                                    # 各保存メソッドは失敗時にFalseを返すか例外を発生させる想定
                                    all_components_successful = False
                                    # エラー発生時は速やかにループを抜けてロールバックさせるため例外をスローすることも検討
                                    raise Exception(
                                        f"{table_name_in_order} の保存処理でエラーが発生しました。"
                                    )
                            executed_operations.add(table_name_in_order)
                else:
                    self.logger.warning(
                        f"レースID {race_id}: DBアクセサにロック順序が定義されていません。"
                        "定義されていない順序で処理を実行します。"
                    )
                    # ロック順序がない場合は、ここに定義されたデフォルトの順序で実行
                    # (ただし、このフォールバックは推奨されない。deadrock.iniでの定義を基本とすべき)
                    default_order = [
                        "players",
                        "entries",
                        "player_records",
                        "line_predictions",
                    ]
                    for table_name_in_default_order in default_order:
                        if table_name_in_default_order in save_operations:
                            condition, operation_func = save_operations[
                                table_name_in_default_order
                            ]
                            if condition:
                                self.logger.debug(
                                    f"デフォルト順序に基づき {table_name_in_default_order} の保存処理を実行します。"
                                )
                                if not operation_func():
                                    all_components_successful = False
                                    raise Exception(
                                        f"{table_name_in_default_order} の保存処理でエラーが発生しました。"
                                    )
                            executed_operations.add(table_name_in_default_order)

                # lock_order に定義されていないが、保存対象の処理が残っていれば実行
                # (save_operationsのキーがdeadrock.iniに全て含まれていれば、このループは通常不要)
                for op_name, (condition, operation_func) in save_operations.items():
                    if op_name not in executed_operations:
                        if condition:
                            self.logger.debug(
                                f"ロック順序外の {op_name} の保存処理を実行します。"
                            )
                            if not operation_func():
                                all_components_successful = False
                                raise Exception(
                                    f"{op_name} の保存処理でエラーが発生しました。"
                                )

                if not all_components_successful:
                    # このメッセージは、上記で例外が発生しなかった場合にのみ到達するが、
                    # 各保存メソッドがFalseを返して例外を発生させない場合に備えて残す
                    self.logger.warning(
                        f"レースID {race_id}: 一部のコンポーネントで保存失敗フラグが立ちました（例外なし）。トランザクションはロールバックされます。"
                    )
                    return False  # トランザクションをロールバックさせる

                self.logger.info(
                    f"レースID {race_id}: Step3データの保存が正常に完了しました（トランザクション内）。"
                )
                return True
            except (
                Exception
            ) as e_tran:  # トランザクション内で発生したすべての例外をキャッチ
                self.logger.error(
                    f"レースID {race_id}: Step3保存トランザクション内でエラー: {e_tran}",
                    exc_info=True,
                )
                # all_components_successful = False # ここでは不要、例外発生で失敗とみなされる
                raise  # execute_in_transaction に例外を伝播させてロールバックさせる
            finally:
                if cursor:
                    cursor.close()

        try:
            return self.accessor.execute_in_transaction(_save_in_transaction)
        except Exception as e:
            self.logger.error(
                f"レースID {race_id} のStep3詳細データトランザクション保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def update_race_step3_status_batch(self, race_ids: List[str], status: str) -> None:
        """
        指定されたレースIDリストの race_status.step3_status を更新する (MySQL用)
        更新前にFOR UPDATEでロックを取得する。
        """
        if not race_ids:
            self.logger.info("更新対象のレースIDがありません (Step3ステータス)。")
            return

        def _update_status_in_transaction(conn):
            cursor = None
            updated_count = 0
            try:
                cursor = conn.cursor(dictionary=True)
                for race_id in race_ids:
                    lock_query = "SELECT race_id, step3_status FROM race_status WHERE race_id = %s FOR UPDATE"
                    locked_row = self.accessor.execute_query_for_update(
                        query=lock_query,
                        params=(race_id,),
                        fetch_one=True,
                        conn=conn,
                        cursor=cursor,
                    )

                    if locked_row:
                        self.logger.debug(
                            f"Race ID {race_id} をロックしました。現在のstep3_status: {locked_row.get('step3_status')}"
                        )
                        update_query = """
                        UPDATE race_status
                        SET step3_status = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE race_id = %s
                        """
                        valid_status = status[:10]
                        self.accessor.execute_many(
                            query=update_query,
                            params_list=[(valid_status, race_id)],
                            existing_conn=conn,
                            existing_cursor=cursor,
                        )
                        updated_count += 1
                        self.logger.info(
                            f"Race ID {race_id} のStep3ステータスを '{valid_status}' に更新準備完了。"
                        )
                    else:
                        self.logger.warning(
                            f"Race ID {race_id} はrace_statusテーブルに存在しないか、ロックできませんでした。"
                        )
                return updated_count
            finally:
                if cursor:
                    cursor.close()

        try:
            num_updated = self.accessor.execute_in_transaction(
                _update_status_in_transaction
            )
            self.logger.info(
                f"合計 {num_updated}/{len(race_ids)} 件のレースのStep3ステータス更新処理が完了しました。"
            )
        except Exception as e:
            self.logger.error(
                f"レースStep3ステータス更新トランザクション中にエラー (IDs: {race_ids}, Status: {status}): {e}",
                exc_info=True,
            )
            raise

    def get_race_statuses(self, race_ids: List[str]) -> Dict[str, str]:
        """
        指定されたレースIDリストに対応する races テーブルの status を取得する。
        Args:
            race_ids: ステータスを取得したいレースIDのリスト。
        Returns:
            race_id をキー、status を値とする辞書。
        """
        if self.accessor is None:
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is None before executing get_race_statuses"
            )
            raise AttributeError("Accessor is None, cannot proceed.")
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step3Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)})"
            )
            raise TypeError(
                f"Accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)})."
            )

        if not race_ids:
            self.logger.info("get_race_statuses: 取得対象のレースIDがありません。")
            return {}

        query = f"""
        SELECT race_id, status
        FROM races
        WHERE race_id IN ({", ".join(["%s"] * len(race_ids))})
        """
        params = tuple(race_ids)

        race_statuses: Dict[str, str] = {}
        try:
            results = self.accessor.execute_query(query, params)
            if results:
                for row in results:
                    race_statuses[str(row["race_id"])] = str(row["status"])
                self.logger.debug(
                    f"get_race_statuses: {len(results)}件のレースステータスを取得しました。"
                )
            else:
                self.logger.info(
                    f"get_race_statuses: 指定されたレースID ({len(race_ids)}件) に該当するレースがracesテーブルに見つかりませんでした。"
                )
        except Exception as e:
            self.logger.error(
                f"get_race_statuses: レースステータス取得中にエラー (IDs: {race_ids}): {e}",
                exc_info=True,
            )
            # エラーが発生しても、部分的に取得できたものは返すか、空を返すか。ここでは空を返す。
            return {}  # もしくはエラーを再raiseする
        return race_statuses

    def _save_players_batch_with_cursor(
        self,
        players_data_list: List[Dict[str, Any]],
        race_id: str,
        batch_size: int,
        conn,
        cursor,
    ) -> bool:
        """
        特定レースに関連する複数の選手情報をまとめて保存/更新 (トランザクション内でcursorを使用)
        """
        if not players_data_list:
            self.logger.info(
                f"(Cursor) レースID {race_id} の保存する選手データがありません。"
            )
            return True

        existing_region_ids = self._get_existing_region_ids()
        if not existing_region_ids:
            self.logger.warning(
                f"(Cursor) レースID {race_id}: regions テーブルから有効な region_id を取得できませんでした。region_id のチェックをスキップします。"
            )

        cols = [
            "race_id",
            "player_id",
            "name",
            "class",
            "player_group",
            "prefecture",
            "term",
            "region_id",
            "yomi",
            "birthday",
            "age",
            "gender",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in cols
            if col not in ["race_id", "player_id"]
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"""
        INSERT INTO players ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        processed_data_count_total = 0
        skipped_count_total = 0
        all_batches_successful = True

        for i in range(0, len(players_data_list), batch_size):
            batch_data_from_updater = players_data_list[i : i + batch_size]
            to_save_in_batch = []
            current_batch_skipped_count = 0

            for p_data_from_updater in batch_data_from_updater:
                player_id = str(p_data_from_updater.get("player_id", ""))
                if not player_id:
                    self.logger.warning(
                        f"(Cursor) 選手データにplayer_idなし。スキップ: {p_data_from_updater}, レースID: {race_id}"
                    )
                    current_batch_skipped_count += 1
                    continue

                region_id_str = str(p_data_from_updater.get("region_id", "")).strip()
                if existing_region_ids and region_id_str not in existing_region_ids:
                    self.logger.warning(
                        f"(Cursor) レースID {race_id}, 選手ID {player_id}: 存在しない region_id ('{region_id_str}') のためスキップ。"
                    )
                    current_batch_skipped_count += 1
                    continue
                elif not region_id_str and existing_region_ids:
                    self.logger.warning(
                        f"(Cursor) レースID {race_id}, 選手ID {player_id}: region_id が空のためスキップ。"
                    )
                    current_batch_skipped_count += 1
                    continue

                raw_gender = p_data_from_updater.get("gender")
                processed_gender: Optional[int] = None
                player_id_for_log = str(p_data_from_updater.get("player_id", "N/A"))
                if isinstance(raw_gender, str):
                    if raw_gender == "男":
                        processed_gender = 1
                    elif raw_gender == "女":
                        processed_gender = 2
                    else:
                        processed_gender = 0
                elif isinstance(raw_gender, int):
                    if raw_gender in (0, 1, 2):
                        processed_gender = raw_gender
                elif raw_gender is not None:
                    self.logger.warning(
                        f"(Cursor) レースID {race_id}, 選手ID {player_id_for_log}: "
                        f"予期しない型の性別値 '{raw_gender}' (型: {type(raw_gender)})。NULLとして扱います。"
                    )

                data = {
                    "race_id": race_id,
                    "player_id": player_id,
                    "name": str(p_data_from_updater.get("name", "")),
                    "class": str(p_data_from_updater.get("class", "")),
                    "player_group": str(p_data_from_updater.get("player_group", "")),
                    "prefecture": str(p_data_from_updater.get("prefecture", "")),
                    "term": p_data_from_updater.get("term"),
                    "region_id": region_id_str if region_id_str else None,
                    "yomi": str(p_data_from_updater.get("yomi", "")),
                    "birthday": self._format_date(p_data_from_updater.get("birthday")),
                    "age": p_data_from_updater.get("age"),
                    "gender": processed_gender,
                }
                to_save_in_batch.append(data)

            skipped_count_total += current_batch_skipped_count
            if not to_save_in_batch:
                if current_batch_skipped_count > 0:
                    self.logger.info(
                        f"(Cursor) レースID {race_id}: この選手データバッチは全件スキップ ({current_batch_skipped_count}件)。"
                    )
                continue

            params_list_batch = [
                tuple(d.get(col) for col in cols) for d in to_save_in_batch
            ]

            try:
                cursor.executemany(query, params_list_batch)
                self.logger.info(
                    f"(Cursor) レースID {race_id}: {len(params_list_batch)}件の選手情報をバッチ保存/更新しました。"
                    f"{f' (スキップ{current_batch_skipped_count}件)' if current_batch_skipped_count > 0 else ''}"
                )
                processed_data_count_total += len(params_list_batch)
            except Exception as e:
                self.logger.error(
                    f"(Cursor) レースID {race_id} の選手情報バッチ保存中にエラー: {e}",
                    exc_info=True,
                )
                all_batches_successful = False
                raise

        if processed_data_count_total > 0:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 合計{processed_data_count_total}件の選手情報を処理。総スキップ数: {skipped_count_total}"
            )
        elif skipped_count_total > 0:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 保存対象の選手情報なし。総スキップ数: {skipped_count_total}"
            )
        return all_batches_successful

    def _save_entries_batch_with_cursor(
        self,
        entries_data_list: List[Dict[str, Any]],
        race_id: str,
        batch_size: int,
        conn,  # conn は execute_many のインターフェース上は直接使われないが、デバッグや将来の拡張用に残すことも考慮
        cursor,
    ) -> bool:
        """
        特定レースの複数の出走表情報をまとめて保存/更新 (トランザクション内でcursorを使用)
        """
        if not entries_data_list:
            self.logger.info(
                f"(Cursor) レースID {race_id} の保存する出走表データがありません。"
            )
            return True

        # 正しいentriesテーブル構造に合わせる
        cols = [
            "number",
            "race_id",
            "absent",
            "player_id",
            "bracket_number",
            "player_current_term_class",
            "player_current_term_group",
            "player_previous_term_class",
            "player_previous_term_group",
            "has_previous_class_group",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in cols
            if col not in ["race_id", "number"]
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"""
        INSERT INTO entries ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        processed_data_count_total = 0
        skipped_count_total = 0
        all_batches_successful = True  # このフラグでバッチ全体の成否を管理

        for i in range(0, len(entries_data_list), batch_size):
            batch_data_from_updater = entries_data_list[i : i + batch_size]
            to_save_in_batch = []
            current_batch_skipped_count = 0

            for entry_data in batch_data_from_updater:
                entry_number = entry_data.get("number")

                if entry_number is None:
                    self.logger.warning(
                        f"(Cursor) 出走表データに車番(number)が不足: {entry_data} for race_id {race_id}"
                    )
                    current_batch_skipped_count += 1
                    continue

                data = {
                    "number": entry_number,
                    "race_id": race_id,
                    "absent": 1 if entry_data.get("absent") else 0,
                    "player_id": entry_data.get("player_id"),
                    "bracket_number": entry_data.get("bracket_number"),
                    "player_current_term_class": entry_data.get(
                        "player_current_term_class"
                    ),
                    "player_current_term_group": entry_data.get(
                        "player_current_term_group"
                    ),
                    "player_previous_term_class": entry_data.get(
                        "player_previous_term_class"
                    ),
                    "player_previous_term_group": entry_data.get(
                        "player_previous_term_group"
                    ),
                    "has_previous_class_group": (
                        1 if entry_data.get("has_previous_class_group") else 0
                    ),
                }
                to_save_in_batch.append(data)

            skipped_count_total += current_batch_skipped_count
            if not to_save_in_batch:
                if current_batch_skipped_count > 0:
                    self.logger.info(
                        f"(Cursor) レースID {race_id}: この出走表データバッチは全件スキップ ({current_batch_skipped_count}件)。"
                    )
                continue  # このバッチはスキップ

            params_list_batch = [
                tuple(d.get(col) for col in cols) for d in to_save_in_batch
            ]
            try:
                cursor.executemany(query, params_list_batch)
                self.logger.info(
                    f"(Cursor) レースID {race_id}: {len(params_list_batch)}件の出走情報をバッチ保存/更新しました。"
                    f"{f' (スキップ{current_batch_skipped_count}件)' if current_batch_skipped_count > 0 else ''}"
                )
                processed_data_count_total += len(params_list_batch)
            except Exception as e:
                self.logger.error(
                    f"(Cursor) レースID {race_id} の出走情報バッチ保存中にエラー: {e}",
                    exc_info=True,
                )
                all_batches_successful = False  # エラーが発生したらフラグをFalseに
                raise  # トランザクション全体を失敗させるために例外を再スロー

        if processed_data_count_total > 0:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 合計{processed_data_count_total}件の出走情報を処理。総スキップ数: {skipped_count_total}"
            )
        elif skipped_count_total > 0:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 保存対象の出走表情報なし。総スキップ数: {skipped_count_total}"
            )
        # all_batches_successful が False の場合、どこかのバッチでエラーが発生している
        return all_batches_successful

    def _save_player_records_batch_with_cursor(
        self,
        player_records_data_list: List[Dict[str, Any]],
        race_id: str,
        batch_size: int,
        conn,  # conn は execute_many のインターフェース上は直接使われないが、デバッグや将来の拡張用に残すことも考慮
        cursor,
    ) -> bool:
        """
        特定レースの複数の選手成績情報 (APIの 'records' 想定) をまとめて保存/更新 (トランザクション内でcursorを使用)
        """
        if not player_records_data_list:
            self.logger.info(
                f"(Cursor) レースID {race_id} の保存する選手成績データがありません。"
            )
            return True

        cols = [
            "race_id",
            "player_id",
            "gear_ratio",
            "style",
            "race_point",
            "comment",
            "prediction_mark",
            "first_rate",
            "second_rate",
            "third_rate",
            "has_modified_gear_ratio",
            "modified_gear_ratio",
            "modified_gear_ratio_str",
            "gear_ratio_str",
            "race_point_str",
            "previous_cup_id",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in cols
            if col not in ["race_id", "player_id"]
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"""
        INSERT INTO player_records ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        processed_data_count_total = 0
        skipped_count_total = 0
        all_batches_successful = True  # バッチ全体の成否を管理

        for i in range(0, len(player_records_data_list), batch_size):
            batch_data_from_updater = player_records_data_list[i : i + batch_size]
            to_save_in_batch = []
            current_batch_skipped_count = 0

            for record_data in batch_data_from_updater:
                player_id = str(record_data.get("player_id", ""))
                if not player_id:
                    self.logger.warning(
                        f"(Cursor) 選手成績データにplayer_idが不足: {record_data} for race_id {race_id}"
                    )
                    current_batch_skipped_count += 1
                    continue

                data = {
                    "race_id": race_id,
                    "player_id": player_id,
                    "gear_ratio": self._safe_float_convert(
                        record_data.get("gear_ratio")
                    ),
                    "style": str(record_data.get("style", "")),
                    "race_point": self._safe_float_convert(
                        record_data.get("race_point")
                    ),
                    "comment": str(record_data.get("comment", "")),
                    "prediction_mark": self._safe_int_convert(
                        record_data.get("prediction_mark")
                    ),
                    "first_rate": self._safe_float_convert(
                        record_data.get("first_rate")
                    ),
                    "second_rate": self._safe_float_convert(
                        record_data.get("second_rate")
                    ),
                    "third_rate": self._safe_float_convert(
                        record_data.get("third_rate")
                    ),
                    "has_modified_gear_ratio": (
                        1 if record_data.get("has_modified_gear_ratio") else 0
                    ),
                    "modified_gear_ratio": self._safe_float_convert(
                        record_data.get("modified_gear_ratio")
                    ),
                    "modified_gear_ratio_str": str(
                        record_data.get("modified_gear_ratio_str", "")
                    ),
                    "gear_ratio_str": str(record_data.get("gear_ratio_str", "")),
                    "race_point_str": str(record_data.get("race_point_str", "")),
                    "previous_cup_id": (
                        str(record_data.get("previous_cup_id", ""))
                        if record_data.get("previous_cup_id")
                        else None
                    ),
                }
                to_save_in_batch.append(data)

            skipped_count_total += current_batch_skipped_count
            if not to_save_in_batch:
                if current_batch_skipped_count > 0:
                    self.logger.info(
                        f"(Cursor) レースID {race_id}: この選手成績データバッチは全件スキップ ({current_batch_skipped_count}件)。"
                    )
                continue  # このバッチはスキップ

            params_list_batch = [
                tuple(d.get(col) for col in cols) for d in to_save_in_batch
            ]
            try:
                cursor.executemany(query, params_list_batch)
                self.logger.info(
                    f"(Cursor) レースID {race_id}: {len(params_list_batch)}件の選手成績情報をバッチ保存/更新しました。"
                    f"{f' (スキップ{current_batch_skipped_count}件)' if current_batch_skipped_count > 0 else ''}"
                )
                processed_data_count_total += len(params_list_batch)
            except Exception as e:
                self.logger.error(
                    f"(Cursor) レースID {race_id} の選手成績情報バッチ保存中にエラー: {e}",
                    exc_info=True,
                )
                all_batches_successful = False  # エラーが発生したらフラグをFalseに
                raise  # トランザクション全体を失敗させるために例外を再スロー

        if processed_data_count_total > 0:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 合計{processed_data_count_total}件の選手成績情報を処理。総スキップ数: {skipped_count_total}"
            )
        elif skipped_count_total > 0:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 保存対象の選手成績情報なし。総スキップ数: {skipped_count_total}"
            )
        return all_batches_successful

    def _save_line_predictions_batch_with_cursor(
        self,
        line_prediction_data: Optional[Dict[str, Any]],
        race_id: str,
        conn,
        cursor,
    ):
        if line_prediction_data is None:
            self.logger.info(
                f"(Cursor) レースID {race_id} のライン予想データがありません。"
            )
            return

        if not isinstance(line_prediction_data, dict):
            self.logger.warning(
                f"(Cursor) レースID {race_id} のライン予想データが予期せぬ型です: {type(line_prediction_data)}"
            )
            return

        # Updaterから整形済みのデータを受け取る
        line_type = str(line_prediction_data.get("lineType", ""))
        line_formation = str(line_prediction_data.get("lineFormation", ""))

        if not line_type and not line_formation:
            self.logger.info(
                f"(Cursor) レースID {race_id}: line_type と line_formation が両方空のため、ライン情報を保存しません。"
            )
            return

        to_save = {
            "race_id": race_id,
            "line_type": line_type,
            "line_formation": line_formation,
        }
        cols = ["race_id", "line_type", "line_formation"]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "race_id"
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"""
        INSERT INTO line_predictions ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = [tuple(to_save.get(col) for col in cols)]

        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Cursor) レースID {race_id}: ライン予想情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) レースID {race_id} のライン予想情報保存中にエラー: {e}",
                exc_info=True,
            )
            raise


# 以下、古いメソッド群は削除 (get_race_data, save_race_info, bulk_save_step3_data,
# _save_chunked_data, _construct_..._insert_query, _prepare_..._params)
