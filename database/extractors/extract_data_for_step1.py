import logging
import threading
from typing import Any, Dict, Optional


class Step1DataExtractor:
    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        ステップ1の初期データを抽出する

        Args:
            start_date (str, optional): 開始日（YYYY-MM-DD形式）
            end_date (str, optional): 終了日（YYYY-MM-DD形式）

        Returns:
            dict: 抽出されたデータ
        """
        thread_id = threading.current_thread().ident
        self.logger.info(f"スレッド {thread_id}: ステップ1の初期データを抽出します")

        extracted_data = {}

        try:
            # 地域テーブルからデータを抽出
            query = "SELECT * FROM regions"
            try:
                regions = self.database.execute_query(query, fetch_all=True)
                extracted_data["regions"] = (
                    [dict(row) for row in regions] if regions else []
                )
                self.logger.info(
                    f"スレッド {thread_id}: {len(extracted_data['regions'])} 件の地域データを抽出しました"
                )
            except Exception as e:
                self.logger.warning(
                    f"スレッド {thread_id}: regions テーブルからのデータ抽出に失敗: {e}"
                )
                extracted_data["regions"] = []

            # 会場テーブルからデータを抽出
            query = "SELECT * FROM venues"
            try:
                venues = self.database.execute_query(query, fetch_all=True)
                extracted_data["venues"] = (
                    [dict(row) for row in venues] if venues else []
                )
                self.logger.info(
                    f"スレッド {thread_id}: {len(extracted_data['venues'])} 件の会場データを抽出しました"
                )
            except Exception as e:
                self.logger.warning(
                    f"スレッド {thread_id}: venues テーブルからのデータ抽出に失敗: {e}"
                )
                extracted_data["venues"] = []

            # 期間内の開催情報を抽出
            if start_date and end_date:
                query = """
                SELECT * FROM cups 
                WHERE (start_date BETWEEN ? AND ?) OR (end_date BETWEEN ? AND ?)
                OR (? BETWEEN start_date AND end_date) OR (? BETWEEN start_date AND end_date)
                """
                params = (
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                    start_date,
                    end_date,
                )
            else:
                # 全件取得
                query = "SELECT * FROM cups"
                params = None

            try:
                cups = self.database.execute_query(query, params=params, fetch_all=True)
                extracted_data["cups"] = [dict(row) for row in cups] if cups else []
                self.logger.info(
                    f"スレッド {thread_id}: {len(extracted_data['cups'])} 件の開催データを抽出しました"
                )
            except Exception as e:
                self.logger.warning(
                    f"スレッド {thread_id}: cups テーブルからのデータ抽出に失敗: {e}"
                )
                extracted_data["cups"] = []

            # 抽出されたデータの件数を合計
            total_records = sum(len(records) for records in extracted_data.values())
            self.logger.info(
                f"スレッド {thread_id}: ステップ1の初期データとして合計 {total_records} 件のレコードを抽出しました"
            )

            return extracted_data

        except Exception as e:
            self.logger.error(
                f"スレッド {thread_id}: データ抽出中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return {}
