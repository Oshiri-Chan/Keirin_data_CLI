import logging
import threading
from typing import Any, Dict, List, Optional  # noqa: F401


class Step3DataExtractor:
    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        cup_id: Optional[str] = None,
        force_update_all: bool = False,
    ) -> Dict[str, List[Dict]]:
        """
        ステップ3の処理に必要なレース識別子を抽出する
        - 期間内の race_id
        - API呼び出しに必要な情報 (cup_id, schedule_id, number)

        Args:
            start_date (str, optional): 開始日（YYYY-MM-DD形式）
            end_date (str, optional): 終了日（YYYY-MM-DD形式）
            cup_id (str, optional): 特定の開催IDを指定する場合。
                                     指定された場合、start_date/end_dateは無視される。
            force_update_all (bool, optional): race_status の step3_status を無視して抽出するかどうか。
                                    Defaults to False.

        Returns:
            dict: 抽出されたデータ {'races_for_update': [{'race_id': str, 'cup_id': str, ...}, ...]}
        """
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"スレッド {thread_id}: ステップ3のデータ抽出を開始します (期間: {start_date} - {end_date}, cup_id: {cup_id}, 強制: {force_update_all})"
        )

        # 本運用ロジック: Step2で保存済みの期間内カップ/レースから抽出
        extracted_data = {"races_for_update": []}
        if not start_date or not end_date:
            self.logger.warning(
                "Step3: 開始日・終了日が指定されていないため抽出をスキップ"
            )
            return extracted_data

        status_condition = ""
        if not force_update_all:
            status_condition = (
                " AND (rs.step3_status != 'completed' OR rs.step3_status IS NULL)"
            )

        query = (
            "SELECT r.race_id, r.cup_id, r.schedule_id, r.number, s.schedule_index AS race_index "
            "FROM races r JOIN schedules s ON r.schedule_id = s.schedule_id "
            "LEFT JOIN race_status rs ON r.race_id = rs.race_id "
            "WHERE DATE(FROM_UNIXTIME(COALESCE(r.start_at, UNIX_TIMESTAMP(s.date)))) BETWEEN %s AND %s"
            f"{status_condition}"
        )
        try:
            rows = self.database.execute_query(query, (start_date, end_date))
            if rows:
                for row in rows:
                    extracted_data["races_for_update"].append(
                        {
                            "race_id": str(row.get("race_id")),
                            "cup_id": str(row.get("cup_id")),
                            "schedule_id": (
                                str(row.get("schedule_id"))
                                if row.get("schedule_id")
                                else None
                            ),
                            "number": row.get("number"),
                            "race_index": row.get("race_index"),
                        }
                    )
            self.logger.info(
                f"スレッド {thread_id}: ステップ3のデータ抽出完了 (更新対象レース: {len(extracted_data['races_for_update'])} 件)"
            )
            return extracted_data
        except Exception as e:
            self.logger.error(f"Step3抽出中にエラー: {e}", exc_info=True)
            return extracted_data

    def _extract_existing_player_ids(self) -> List[str]:
        """既存のプレイヤーIDリストを抽出するヘルパーメソッド（例）"""
        try:
            query = "SELECT player_id FROM players"
            players = self.database.execute_query(query)
            return [p["player_id"] for p in players] if players else []
        except Exception as e:
            self.logger.warning(f"既存プレイヤーIDの抽出に失敗: {e}")
            return []
