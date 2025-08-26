import logging
import threading
from typing import Any, Dict, List, Optional  # noqa: F401


class Step2DataExtractor:
    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        venue_codes: Optional[List[str]] = None,
        force_update_all: bool = False,
    ) -> Dict[str, Any]:
        """
        ステップ2の処理に必要なデータを抽出する
        - 期間内の cup_id リスト
        - (オプション) 既存のレース、出走情報など

        Args:
            start_date (str, optional): 開始日（YYYY-MM-DD形式）
            end_date (str, optional): 終了日（YYYY-MM-DD形式）
            venue_codes (List[str], optional): 対象の会場コード（開催ID）リスト
            force_update_all (bool, optional): 全データを強制更新するかどうか

        Returns:
            dict: 抽出されたデータ {'cup_ids_for_update': [...], 'existing_races': [...], ...}
        """
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"スレッド {thread_id}: ステップ2のデータ抽出を開始します (期間: {start_date} - {end_date}, 会場: {venue_codes}, 強制: {force_update_all})"
        )

        # 本運用ロジックに復帰
        extracted_data = {
            "cup_ids_for_update": [],
            "existing_races": [],
            "existing_entries": [],
            # 必要に応じて他のテーブルのデータも追加
        }

        try:
            # --- venue_codes 指定の場合 ---
            if venue_codes:
                self.logger.info(
                    f"スレッド {thread_id}: 指定された会場コード {venue_codes} のCup IDを抽出します。"
                )
                # venue_codes が指定されている場合、それらをそのまま cup_ids として使用
                extracted_data["cup_ids_for_update"] = venue_codes
                self.logger.info(
                    f"スレッド {thread_id}: 指定会場の Cup ID {len(extracted_data['cup_ids_for_update'])} 件を設定しました"
                )

            # --- 期間内の cup_id を抽出 ---
            elif start_date and end_date:
                # ステータス条件を追加（force_update_all が False の場合のみ）
                status_condition = ""
                if not force_update_all:
                    status_condition = (
                        " AND EXISTS ("
                        " SELECT 1 FROM schedules s"
                        " JOIN races r ON s.schedule_id = r.schedule_id"
                        " LEFT JOIN race_status rs ON r.race_id = rs.race_id"
                        " WHERE s.cup_id = cups.cup_id"
                        " AND (rs.step3_status IS NULL OR rs.step3_status = 'pending')"
                        " )"
                    )

                cup_query = (
                    "SELECT DISTINCT cup_id "
                    "FROM cups "
                    "WHERE (start_date BETWEEN %s AND %s) OR (end_date BETWEEN %s AND %s) "
                    "   OR (%s BETWEEN start_date AND end_date) OR (%s BETWEEN start_date AND end_date)"
                    f"{status_condition}"
                )
                # パラメータをタプルで渡す
                cup_params = (
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                )
                try:
                    cups_result = self.database.execute_query(
                        cup_query, params=cup_params
                    )
                    extracted_data["cup_ids_for_update"] = (
                        [row["cup_id"] for row in cups_result] if cups_result else []
                    )
                    status_msg = (
                        "(強制更新モード)" if force_update_all else "未処理 (Step2)"
                    )
                    self.logger.info(
                        f"スレッド {thread_id}: 更新対象の Cup ID {len(extracted_data['cup_ids_for_update'])} 件{status_msg}を抽出しました"
                    )
                except Exception as e:
                    self.logger.error(
                        f"スレッド {thread_id}: cups テーブルからの cup_id 抽出に失敗: {e}"
                    )
                    extracted_data["cup_ids_for_update"] = []
            else:
                self.logger.warning(
                    f"スレッド {thread_id}: 開始日・終了日または会場コードが指定されていないため、cup_id の抽出をスキップします。"
                )

            # --- (オプション) 既存のレース情報を抽出 ---
            # 必要に応じて、期間内の既存レース情報を取得するロジックを追加
            # 例: (既存のコードを流用・修正)
            # if start_date and end_date:
            #     race_query = "SELECT * FROM races WHERE date BETWEEN ? AND ?"
            #     race_params = (start_date, end_date)
            #     try:
            #         races = self.database.execute_query(race_query, params=race_params, fetch_all=True)
            #         extracted_data['existing_races'] = [dict(row) for row in races] if races else []
            #         self.logger.info(f"スレッド {thread_id}: 既存レースデータ {len(extracted_data['existing_races'])} 件を抽出")
            #     except Exception as e:
            #         self.logger.warning(f"スレッド {thread_id}: races テーブルからのデータ抽出に失敗: {e}")
            # else:
            #     self.logger.info(f"スレッド {thread_id}: 期間指定がないため既存レース情報の抽出はスキップ")
            #
            # --- (オプション) 既存の出走情報を抽出 ---
            # 同様に既存の出走情報なども抽出可能

            total_cups = len(extracted_data["cup_ids_for_update"])
            self.logger.info(
                f"スレッド {thread_id}: ステップ2のデータ抽出完了 (更新対象 Cup ID: {total_cups} 件)"
            )

            return extracted_data

        except Exception as e:
            self.logger.error(
                f"スレッド {thread_id}: データ抽出中に予期せぬエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            # エラー発生時は空のデータを返す
            return {
                "cup_ids_for_update": [],
                "existing_races": [],
                "existing_entries": [],
            }
