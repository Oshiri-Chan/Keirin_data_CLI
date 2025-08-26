"""
ステップ1: 月間開催情報のデータセーバー
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional  # 型ヒントのため追加

# KeirinDataAccessorをインポートする想定
from database.db_accessor import KeirinDataAccessor  # パスは環境に合わせてください

# import pandas as pd # pandas は使用しないので削除


class Step1Saver:
    """
    ステップ1: 月間開催情報を保存するクラス (MySQL対応)
    """

    def __init__(
        self, accessor: KeirinDataAccessor, logger: logging.Logger = None
    ):  # db_instance を accessor に変更し型ヒント追加
        """
        初期化

        Args:
            accessor (KeirinDataAccessor): データベースアクセサーインスタンス
            logger (logging.Logger, optional): ロガーオブジェクト。 Defaults to None.
        """
        self.accessor = accessor  # KeirinDataAccessor のインスタンスを保持
        self.logger = logger or logging.getLogger(__name__)

    def save_regions_batch(self, regions_data: List[Dict[str, Any]]):
        """
        複数の地域情報をまとめて保存/更新 (MySQL用)

        Args:
            regions_data (List[Dict[str, Any]]): 地域情報の辞書のリスト
                各辞書に必要なキー: 'id' (地域ID), 'name' (地域名)
        """
        if not regions_data:
            self.logger.info("保存する地域データがありません。")
            return

        to_save = []
        for region_data_from_updater in regions_data:  # 変数名変更
            # Updaterから渡されるキー名に合わせる
            region_id = region_data_from_updater.get("region_id")
            region_name = region_data_from_updater.get("region_name")

            if region_id and region_name:
                to_save.append(
                    {
                        "region_id": str(region_id),
                        "region_name": str(region_name),
                        # 'updated_at' はMySQL側で自動更新
                    }
                )
            else:
                # この警告が出るのは、Updater側で 'region_id' や 'region_name' が欠損している場合のみになる
                self.logger.warning(
                    f"地域データに必要な情報が不足しています (Updaterからのデータ確認): {region_data_from_updater}"
                )

        if not to_save:
            self.logger.info("整形後、保存対象の地域データがありませんでした。")
            return

        query = """
        INSERT INTO regions (region_id, region_name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE region_name = VALUES(region_name)
        """
        params_list = [(data["region_id"], data["region_name"]) for data in to_save]

        self.logger.info(f"地域情報保存開始: {len(params_list)}件のデータ")
        self.logger.debug(
            f"地域情報データ例: {params_list[:2] if params_list else 'なし'}"
        )
        self.logger.debug(f"使用クエリ: {query}")

        try:
            self.logger.info("execute_many実行中...")
            affected = self.accessor.execute_many(query, params_list)
            self.logger.info(f"execute_many完了! 影響行数: {affected}")
            self.logger.info("地域情報の保存処理が正常に完了しました。")
        except Exception as e:
            self.logger.error(
                f"地域情報の保存中にエラーが発生しました: {e}", exc_info=True
            )
            self.logger.error(f"失敗したクエリ: {query}")
            self.logger.error(
                f"失敗したデータ: {params_list[:3] if params_list else 'なし'}"
            )
            raise

    def save_venues_batch(self, venues_data: List[Dict[str, Any]]):
        """
        複数の会場情報をまとめて保存/更新 (MySQL用)

        Args:
            venues_data (List[Dict[str, Any]]): 会場情報の辞書のリスト
        """
        if not venues_data:
            self.logger.info("保存する会場データがありません。")
            return

        to_save = []
        for venue_api_data in venues_data:
            venue_id = str(venue_api_data.get("venue_id", venue_api_data.get("id", "")))
            venue_name = str(
                venue_api_data.get("venue_name", venue_api_data.get("name", ""))
            )

            if not venue_id or not venue_name:
                self.logger.warning(
                    f"会場データにIDまたは名称がありません: {venue_api_data}"
                )
                continue

            data = {
                "venue_id": venue_id,
                "venue_name": venue_name,
                "name1": str(venue_api_data.get("name1", "")),
                "address": str(venue_api_data.get("address", "")),
                "phoneNumber": str(
                    venue_api_data.get(
                        "phoneNumber", venue_api_data.get("phone_number", "")
                    )
                ),
                "websiteUrl": str(
                    venue_api_data.get("websiteUrl", venue_api_data.get("url", ""))
                ),
                "bankFeature": str(venue_api_data.get("bankFeature", "")),
                "trackStraightDistance": (
                    venue_api_data.get("trackStraightDistance")
                    if venue_api_data.get("trackStraightDistance") is not None
                    else None
                ),
                "trackAngleCenter": str(venue_api_data.get("trackAngleCenter", "")),
                "trackAngleStraight": str(venue_api_data.get("trackAngleStraight", "")),
                "homeWidth": (
                    venue_api_data.get("homeWidth")
                    if venue_api_data.get("homeWidth") is not None
                    else None
                ),
                "backWidth": (
                    venue_api_data.get("backWidth")
                    if venue_api_data.get("backWidth") is not None
                    else None
                ),
                "centerWidth": (
                    venue_api_data.get("centerWidth")
                    if venue_api_data.get("centerWidth") is not None
                    else None
                ),
            }
            to_save.append(data)

        if not to_save:
            self.logger.info("整形後、保存対象の会場データがありませんでした。")
            return

        cols = [
            "venue_id",
            "venue_name",
            "name1",
            "address",
            "phoneNumber",
            "websiteUrl",
            "bankFeature",
            "trackStraightDistance",
            "trackAngleCenter",
            "trackAngleStraight",
            "homeWidth",
            "backWidth",
            "centerWidth",
        ]

        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "venue_id"
        ]
        update_sql = ", ".join(update_sql_parts)

        query = f"""
        INSERT INTO venues ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = []
        for data_dict in to_save:
            params_list.append(tuple(data_dict.get(col) for col in cols))

        try:
            affected = self.accessor.execute_many(query, params_list)
            self.logger.info(
                f"会場情報の保存処理が正常に完了しました。影響行数: {affected}"
            )
        except Exception as e:
            self.logger.error(
                f"会場情報の保存中にエラーが発生しました: {e}", exc_info=True
            )
            raise

    def save_cups_batch(self, cups_data: List[Dict[str, Any]]):
        """
        複数のカップ情報をまとめて保存/更新 (MySQL用)

        Args:
            cups_data (List[Dict[str, Any]]): カップ情報の辞書のリスト
        """
        if not cups_data:
            self.logger.info("保存するカップデータがありません。")
            return

        to_save = []
        for cup_api_data in cups_data:
            cup_id = str(cup_api_data.get("cup_id", cup_api_data.get("id", "")))
            cup_name = str(cup_api_data.get("cup_name", cup_api_data.get("name", "")))

            if not cup_id or not cup_name:
                self.logger.warning(
                    f"カップデータにIDまたは名称がありません: {cup_api_data}"
                )
                continue

            start_date_str = cup_api_data.get(
                "start_date", cup_api_data.get("startDate")
            )
            end_date_str = cup_api_data.get("end_date", cup_api_data.get("endDate"))

            start_date = self._format_date(start_date_str)
            end_date = self._format_date(end_date_str)

            players_unfixed_val = cup_api_data.get(
                "players_unfixed", cup_api_data.get("playersUnfixed")
            )
            players_unfixed = (
                1 if players_unfixed_val else 0
            )  # MySQL BOOLEAN は TINYINT(1)

            duration_val = cup_api_data.get("duration", cup_api_data.get("days"))
            duration = int(duration_val) if duration_val is not None else None

            grade_val = cup_api_data.get("grade")
            grade = int(grade_val) if grade_val is not None else None

            # labelsを適切にカンマ区切り文字列に変換
            labels_raw = cup_api_data.get("labels", [])
            if isinstance(labels_raw, list):
                labels = ",".join(str(label) for label in labels_raw)
            else:
                labels = str(labels_raw) if labels_raw else ""

            data = {
                "cup_id": cup_id,
                "cup_name": cup_name,
                "start_date": start_date,
                "end_date": end_date,
                "duration": duration,
                "grade": grade,
                "venue_id": str(
                    cup_api_data.get("venue_id", cup_api_data.get("venueId", ""))
                ),
                "labels": labels,
                "players_unfixed": players_unfixed,
            }
            to_save.append(data)

        if not to_save:
            self.logger.info("整形後、保存対象のカップデータがありませんでした。")
            return

        cols = [
            "cup_id",
            "cup_name",
            "start_date",
            "end_date",
            "duration",
            "grade",
            "venue_id",
            "labels",
            "players_unfixed",
        ]

        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "cup_id"
        ]
        update_sql = ", ".join(update_sql_parts)

        query = f"""
        INSERT INTO cups ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = []
        for data_dict in to_save:
            params_list.append(tuple(data_dict.get(col) for col in cols))

        try:
            affected = self.accessor.execute_many(query, params_list)
            self.logger.info(
                f"カップ情報の保存処理が正常に完了しました。影響行数: {affected}"
            )
        except Exception as e:
            self.logger.error(
                f"カップ情報の保存中にエラーが発生しました: {e}", exc_info=True
            )
            raise

    def _format_date(self, date_str: Any) -> Optional[str]:
        """日付文字列を YYYY-MM-DD 形式に変換、不正な場合はNoneを返す"""
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            # YYYYMMDD 形式を試す
            if len(date_str) == 8 and date_str.isdigit():
                return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
            # YYYY-MM-DD HH:MM:SS (または YYYY-MM-DD) 形式を試す
            dt_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt_obj.strftime("%Y-%m-%d")
        except ValueError:
            # YYYY-MM-DD 単独の形式も試す
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                self.logger.warning(f"不正な日付形式です: {date_str}")
                return None

    def save_monthly_cups(self, monthly_data: Dict[str, Any]):
        """
        月間開催情報をアトミックに保存するメソッド。
        regions, venues, cups のデータをトランザクション内で処理する。
        """
        if not monthly_data or "month" not in monthly_data:
            self.logger.error(
                "月間開催情報データが不正です。'month' キーが見つかりません。"
            )
            return False, []

        month_content = monthly_data.get("month", {})
        if not month_content:
            self.logger.warning("月間開催情報の 'month' の内容が空です。")
            return True, []

        try:
            # execute_in_transaction を使用してアトミックな処理を実行
            # _atomic_save_monthly_cups_internal のようなヘルパーメソッドを定義して呼び出す
            # このヘルパーメソッドが conn と cursor を受け取る
            success, saved_cup_ids = self.accessor.execute_in_transaction(
                self._atomic_save_monthly_cups_internal, month_content
            )
            if success:
                self.logger.info("月間開催情報のアトミックな保存が完了しました。")
            else:
                self.logger.error("月間開催情報のアトミックな保存に失敗しました。")
            return success, saved_cup_ids
        except Exception as e:
            self.logger.error(
                f"月間開催情報のアトミックな保存処理中に予期せぬエラー: {e}",
                exc_info=True,
            )
            return False, []

    def _atomic_save_monthly_cups_internal(self, conn, month_content: Dict[str, Any]):
        """
        月間開催情報を実際に保存する内部メソッド（トランザクション内で実行される）。
        Args:
            conn: データベース接続オブジェクト
            month_content (Dict[str, Any]): APIから取得した月間開催データの 'month' 部分
        Returns:
            Tuple[bool, List[str]]: 成功したかどうか、保存されたカップIDのリスト（現状は空）
        """
        saved_cup_ids = []
        all_success = True
        cursor = None

        try:
            cursor = conn.cursor(dictionary=True)

            # 地域情報の保存
            regions_api_data = month_content.get("regions", [])
            if regions_api_data:
                try:
                    self._atomic_save_regions(conn, cursor, regions_api_data)
                    self.logger.info("地域情報をアトミックに保存しました。")
                except Exception as e:
                    self.logger.error(
                        f"アトミックな地域情報保存中にエラー: {e}", exc_info=True
                    )
                    all_success = False
                    raise
            else:
                self.logger.info("月間開催情報に地域データがありませんでした。")

            # 会場情報の保存
            venues_api_data = month_content.get("venues", [])
            if venues_api_data:
                try:
                    self._atomic_save_venues(conn, cursor, venues_api_data)
                    self.logger.info("会場情報をアトミックに保存しました。")
                except Exception as e:
                    self.logger.error(
                        f"アトミックな会場情報保存中にエラー: {e}", exc_info=True
                    )
                    all_success = False
                    raise
            else:
                self.logger.info("月間開催情報に会場データがありませんでした。")

            # カップ情報の保存
            cups_api_data = month_content.get("cups", [])
            if cups_api_data:
                try:
                    self._atomic_save_cups(conn, cursor, cups_api_data)
                    self.logger.info("カップ情報をアトミックに保存しました。")
                except Exception as e:
                    self.logger.error(
                        f"アトミックなカップ情報保存中にエラー: {e}", exc_info=True
                    )
                    all_success = False
                    raise

            return all_success, saved_cup_ids
        finally:
            if cursor:
                cursor.close()

    def _atomic_save_regions(self, conn, cursor, regions_data: List[Dict[str, Any]]):
        if not regions_data:
            self.logger.info("(Atomic) 保存する地域データがありません。")
            return

        to_save = []
        for region_data_from_updater in regions_data:
            region_id = region_data_from_updater.get(
                "region_id", region_data_from_updater.get("id")
            )  # APIのキー名揺れに対応
            region_name = region_data_from_updater.get(
                "region_name", region_data_from_updater.get("name")
            )

            if region_id and region_name:
                to_save.append(
                    {
                        "region_id": str(region_id),
                        "region_name": str(region_name),
                    }
                )
            else:
                self.logger.warning(
                    f"(Atomic) 地域データに必要な情報が不足しています: {region_data_from_updater}"
                )

        if not to_save:
            self.logger.info(
                "(Atomic) 整形後、保存対象の地域データがありませんでした。"
            )
            return

        query = """
        INSERT INTO regions (region_id, region_name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE region_name = VALUES(region_name)
        """
        params_list = [(data["region_id"], data["region_name"]) for data in to_save]

        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Atomic) {len(params_list)}件の地域情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(f"(Atomic) 地域情報の保存中にエラー: {e}", exc_info=True)
            raise

    def _atomic_save_venues(self, conn, cursor, venues_data: List[Dict[str, Any]]):
        if not venues_data:
            self.logger.info("(Atomic) 保存する会場データがありません。")
            return

        to_save = []
        for venue_api_data in venues_data:
            venue_id = str(venue_api_data.get("venue_id", venue_api_data.get("id", "")))
            venue_name = str(
                venue_api_data.get("venue_name", venue_api_data.get("name", ""))
            )

            if not venue_id or not venue_name:
                self.logger.warning(
                    f"(Atomic) 会場データにIDまたは名称がありません: {venue_api_data}"
                )
                continue

            data = {
                "venue_id": venue_id,
                "venue_name": venue_name,
                "name1": str(
                    venue_api_data.get("name1", "")
                ),  # venue_short_name に相当する可能性
                "address": str(venue_api_data.get("address", "")),
                "phoneNumber": str(
                    venue_api_data.get(
                        "phoneNumber", venue_api_data.get("phone_number", "")
                    )
                ),
                "websiteUrl": str(
                    venue_api_data.get("websiteUrl", venue_api_data.get("url", ""))
                ),  # key が揺れる場合があるので .get で取得
                "bankFeature": str(venue_api_data.get("bankFeature", "")),
                "trackStraightDistance": venue_api_data.get("trackStraightDistance"),
                "trackAngleCenter": str(venue_api_data.get("trackAngleCenter", "")),
                "trackAngleStraight": str(venue_api_data.get("trackAngleStraight", "")),
                "homeWidth": venue_api_data.get("homeWidth"),
                "backWidth": venue_api_data.get("backWidth"),
                "centerWidth": venue_api_data.get("centerWidth"),
                # 'region_id' は venues テーブルのスキーマにあれば追加する
                "region_id": str(
                    venue_api_data.get("regionId", "")
                ),  # APIのキー名 'regionId' を想定
            }
            to_save.append(data)

        if not to_save:
            self.logger.info(
                "(Atomic) 整形後、保存対象の会場データがありませんでした。"
            )
            return

        cols = [
            "venue_id",
            "venue_name",
            "name1",
            "address",
            "phoneNumber",
            "websiteUrl",
            "bankFeature",
            "trackStraightDistance",
            "trackAngleCenter",
            "trackAngleStraight",
            "homeWidth",
            "backWidth",
            "centerWidth",
            "region_id",  # region_id を追加
        ]
        # Noneの値をSQLのNULLとして扱うために %s を使う
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "venue_id"
        ]
        update_sql = ", ".join(update_sql_parts)

        query = f"""
        INSERT INTO venues ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = []
        for data_dict in to_save:
            params_list.append(tuple(data_dict.get(col) for col in cols))

        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Atomic) {len(params_list)}件の会場情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(f"(Atomic) 会場情報の保存中にエラー: {e}", exc_info=True)
            raise

    def _atomic_save_cups(self, conn, cursor, cups_data: List[Dict[str, Any]]):
        if not cups_data:
            self.logger.info("(Atomic) 保存するカップデータがありません。")
            return

        to_save = []
        for cup_api_data in cups_data:
            cup_id = str(cup_api_data.get("cup_id", cup_api_data.get("id", "")))
            cup_name = str(cup_api_data.get("cup_name", cup_api_data.get("name", "")))

            if not cup_id or not cup_name:
                self.logger.warning(
                    f"(Atomic) カップデータにIDまたは名称がありません: {cup_api_data}"
                )
                continue

            start_date_str = cup_api_data.get(
                "start_date", cup_api_data.get("startDate")
            )
            end_date_str = cup_api_data.get("end_date", cup_api_data.get("endDate"))

            start_date = self._format_date(start_date_str)
            end_date = self._format_date(end_date_str)

            players_unfixed_val = cup_api_data.get(
                "players_unfixed", cup_api_data.get("playersUnfixed")
            )
            players_unfixed = 1 if players_unfixed_val else 0

            duration_val = cup_api_data.get(
                "duration", cup_api_data.get("days")
            )  # APIによってキー名が 'days' の場合も考慮
            duration = int(duration_val) if duration_val is not None else None

            grade_val = cup_api_data.get("grade")
            grade = int(grade_val) if grade_val is not None else None

            # APIからの venueId を使用
            venue_id_val = cup_api_data.get("venue_id", cup_api_data.get("venueId"))

            # labelsを適切にカンマ区切り文字列に変換
            labels_raw = cup_api_data.get("labels", [])
            if isinstance(labels_raw, list):
                labels = ",".join(str(label) for label in labels_raw)
            else:
                labels = str(labels_raw) if labels_raw else ""

            data = {
                "cup_id": cup_id,
                "cup_name": cup_name,
                "start_date": start_date,
                "end_date": end_date,
                "duration": duration,
                "grade": grade,
                "venue_id": (
                    str(venue_id_val) if venue_id_val else None
                ),  # venue_id がなければ None
                "labels": labels,
                "players_unfixed": players_unfixed,
                # "schedule_id": schedule_id, # schedule_id は呼び出し元で紐付けるか、別途渡す必要がある
            }
            to_save.append(data)

        if not to_save:
            self.logger.info(
                "(Atomic) 整形後、保存対象のカップデータがありませんでした。"
            )
            return

        cols = [
            "cup_id",
            "cup_name",
            "start_date",
            "end_date",
            "duration",
            "grade",
            "venue_id",
            "labels",
            "players_unfixed",  # schedule_id は一旦除外
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "cup_id"
        ]
        update_sql = ", ".join(update_sql_parts)

        query = f"""
        INSERT INTO cups ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = []
        for data_dict in to_save:
            params_list.append(tuple(data_dict.get(col) for col in cols))

        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Atomic) {len(params_list)}件のカップ情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Atomic) カップ情報の保存中にエラー: {e}", exc_info=True
            )
            raise
