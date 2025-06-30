"""
Winticketデータ保存の基底サービス
"""

import logging
from datetime import datetime


class WinticketBaseSaver:
    """
    Winticketのデータ保存の基底クラス - 共通機能を提供
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
        self.logger.info(f"{self.__class__.__name__}が初期化されました")

    def get_current_timestamp(self):
        """
        現在のタイムスタンプを取得

        Returns:
            str: 現在のタイムスタンプ（YYYY-MM-DD HH:MM:SS形式）
        """
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def save_to_database(self, df, table_name, primary_keys, format="csv"):
        """
        データフレームをデータベースに保存

        Args:
            df (DataFrame): 保存するデータフレーム
            table_name (str): テーブル名
            primary_keys (list): プライマリキーのリスト
            format (str): 一時ファイル形式（'csv'または'parquet'）

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            if df.empty:
                self.logger.warning(
                    f"{table_name}のデータが空のため保存をスキップします"
                )
                return True

            # 一時ファイル経由でデータを保存
            success = self.db.process_with_temp_file(
                df, table_name, primary_keys, format=format
            )

            if success:
                self.logger.info(f"{len(df)}件の{table_name}データを保存しました")
            else:
                self.logger.error(f"{table_name}データの保存に失敗しました")

            return success

        except Exception as e:
            self.logger.error(
                f"{table_name}データの保存中にエラー: {str(e)}", exc_info=True
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
