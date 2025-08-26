import logging
import threading
from typing import Any, Dict, List, Optional  # noqa: F401


class Step5DataExtractor:
    """
    Step 5: レース結果取得に必要な情報をデータベースから抽出するクラス。
    yen-joy.net の URL 構築に必要な情報を races, schedules, cups テーブルから取得する。
    """

    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self,
        start_date_filter: Optional[str] = None,
        end_date_filter: Optional[str] = None,
        force: bool = False,
    ) -> List[Dict]:
        """
        指定された日付範囲のレース情報から、結果取得用 URL 構築に必要な情報を抽出する。

        Args:
            start_date_filter (Optional[str]): 抽出対象のレース開催日の開始日 (YYYY-MM-DD)
            end_date_filter (Optional[str]): 抽出対象のレース開催日の終了日 (YYYY-MM-DD)
            force (bool, optional): 処理済み (lap_data_status.is_processed=1) のレースも強制的に抽出するかどうか。
                                    Defaults to False.

        Returns:
            List[Dict]: 抽出されたレース情報のリスト。各辞書には URL 構築に必要なキーが含まれる。
                        日付範囲が無効またはデータがない場合は空リスト。
        """
        thread_id = threading.get_ident()
        self.logger.info(
            f"[Thread-{thread_id}] Step 5 データ抽出開始。"
            f"期間: {start_date_filter} - {end_date_filter}, 強制: {force}"
        )

        # 本運用: DBから対象レースを抽出
        if not start_date_filter or not end_date_filter:
            self.logger.warning(
                f"[Thread-{thread_id}] Step 5 抽出条件が不十分です。start={start_date_filter}, end={end_date_filter}"
            )
            return []

        try:
            # KeirinDataAccessor 実装に用意された取得関数があれば優先利用
            if hasattr(self.database, "get_yenjoy_races_to_update_for_step5"):
                rows = self.database.get_yenjoy_races_to_update_for_step5(
                    start_date_filter,
                    end_date_filter,
                    venue_codes=None,
                    force_update_all=force,
                )
                self.logger.info(
                    f"[Thread-{thread_id}] Step 5 抽出: {len(rows) if rows else 0} 件"
                )
                return rows or []

            # フォールバック: 直接SQLで抽出
            sql = (
                "SELECT r.race_id, "
                "COALESCE(DATE_FORMAT(FROM_UNIXTIME(r.start_at),'%Y-%m-%d'), DATE_FORMAT(STR_TO_DATE(s.date,'%Y%m%d'),'%Y-%m-%d')) AS race_date_db, "
                "c.venue_id AS venue_code, r.number AS race_number, "
                "s.date AS race_date_yyyymmdd, DATE_FORMAT(c.start_date,'%Y%m%d') AS cup_start_date_yyyymmdd "
                "FROM races r JOIN schedules s ON r.schedule_id=s.schedule_id "
                "JOIN cups c ON s.cup_id=c.cup_id "
                "LEFT JOIN lap_data_status lds ON r.race_id = lds.race_id "
                "WHERE DATE(FROM_UNIXTIME(COALESCE(r.start_at, UNIX_TIMESTAMP(STR_TO_DATE(s.date,'%Y%m%d'))))) BETWEEN %s AND %s "
                "AND (lds.is_processed = 0 OR lds.race_id IS NULL) "
                "ORDER BY r.start_at, c.venue_id, r.number"
            )
            params = (start_date_filter, end_date_filter)
            rows = self.database.execute_query(sql, params)
            self.logger.info(
                f"[Thread-{thread_id}] Step 5 抽出(SQL直): {len(rows) if rows else 0} 件"
            )
            return rows or []
        except Exception as e:
            self.logger.error(
                f"[Thread-{thread_id}] Step 5 抽出中にエラー: {e}", exc_info=True
            )
            return []
