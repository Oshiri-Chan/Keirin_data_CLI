import logging
import threading
from typing import Any, Dict, List, Optional  # noqa: F401


class Step4DataExtractor:
    """
    Step 4 の更新に必要なデータをデータベースから抽出するクラス。
    主に races テーブルと schedules テーブルから race_id と日付を取得する。
    """

    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        cup_id: Optional[str] = None,
        force_update_all: bool = False,
    ) -> List[Dict]:
        """
        指定された日付範囲または開催IDのレース情報を抽出する。
        Step 4 の API 呼び出しに必要な情報を返す (race_id, cup_id, number, race_index など)。

        Args:
            start_date (Optional[str]): 開始日 (YYYY-MM-DD)
            end_date (Optional[str]): 終了日 (YYYY-MM-DD)
            cup_id (Optional[str]): 特定の開催IDを指定する場合。
                                     指定された場合、start_date/end_dateは無視される。
            force_update_all (bool, optional): race_status の step4_status を無視して抽出するかどうか。
                                    Defaults to False.

        Returns:
            List[Dict]: 抽出されたレース情報のリスト。各辞書には必要な情報が含まれる。
                        条件に合うデータがない場合は空リスト。
        """
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"[Thread-{thread_id}] Step 4 データ抽出開始。"
            f"期間: {start_date} - {end_date}, cup_id: {cup_id}, 強制: {force_update_all}"
        )

        # 本運用ロジック: 期間からracesを抽出（未完了のみ）
        results: List[Dict] = []
        if not start_date or not end_date:
            self.logger.warning(
                f"[Thread-{thread_id}] Step4: 期間未指定のため抽出をスキップします"
            )
            return results

        status_condition = ""
        if not force_update_all:
            status_condition = (
                " AND (rs.step4_status != 'completed' OR rs.step4_status IS NULL)"
            )

        query = (
            "SELECT r.race_id, r.cup_id, r.schedule_id, r.number, "
            "DATE_FORMAT(FROM_UNIXTIME(r.start_at), '%Y%m%d') AS date_ymd, "
            "s.schedule_index AS race_index "
            "FROM races r JOIN schedules s ON r.schedule_id = s.schedule_id "
            "LEFT JOIN race_status rs ON r.race_id = rs.race_id "
            "WHERE DATE(FROM_UNIXTIME(COALESCE(r.start_at, UNIX_TIMESTAMP(s.date)))) BETWEEN %s AND %s"
            f"{status_condition}"
        )
        try:
            rows = self.database.execute_query(query, (start_date, end_date))
            for row in rows or []:
                results.append(
                    {
                        "race_id": str(row.get("race_id")),
                        "cup_id": str(row.get("cup_id")),
                        "schedule_id": (
                            str(row.get("schedule_id"))
                            if row.get("schedule_id")
                            else None
                        ),
                        "number": row.get("number"),
                        "date": str(row.get("date_ymd")),
                        "race_index": row.get("race_index"),
                        "race_table_status": None,
                    }
                )
            self.logger.info(
                f"[Thread-{thread_id}] Step 4 データ抽出完了。{len(results)} 件のレース情報を取得しました。"
            )
            return results
        except Exception as e:
            self.logger.error(
                f"[Thread-{thread_id}] Step4抽出中にエラー: {e}", exc_info=True
            )
            return []

    # Step 4 で他に事前にDBから取得しておきたい情報があれば、ここに追加メソッドを定義
    # 例: 既存のオッズデータをチェックするなど
    # def _extract_existing_odds_info(self, race_ids: List[str]) -> Set[str]:
    #     # ... 実装 ...
    #     pass
