"""
データベースクエリ実行の基本機能を提供するモジュール
"""

import logging
import threading

import pandas as pd


class QueryExecutor:
    """
    SQLクエリの実行と結果取得のための機能を提供するクラス
    """

    def __init__(self, db_connector, logger=None):
        """
        初期化

        Args:
            db_connector: データベース接続オブジェクト
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.db = db_connector
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("QueryExecutorが初期化されました")

    def execute(self, query, params=None):
        """
        SQLクエリを実行する（結果を返さない操作用）

        Args:
            query (str): 実行するSQLクエリ
            params (tuple, list, dict, optional): クエリのパラメータ

        Returns:
            bool: 実行成功の場合はTrue
        """
        thread_id = threading.current_thread().ident

        try:
            conn = self.db.connect()
            cursor = conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if not self.db._transaction_active:
                conn.commit()

            self.logger.debug(
                f"スレッド {thread_id}: クエリを実行しました: {query[:100]}..."
            )
            return True

        except Exception as e:
            self.logger.error(f"スレッド {thread_id}: クエリ実行エラー: {e}")
            self.logger.debug(f"実行しようとしたクエリ: {query}")
            if params:
                self.logger.debug(f"パラメータ: {params}")

            if not self.db._transaction_active:
                conn.rollback()

            return False

    def executemany(self, query, params_list):
        """
        複数のパラメータセットでSQLクエリを実行する

        Args:
            query (str): 実行するSQLクエリ
            params_list (list): パラメータのリスト

        Returns:
            bool: 実行成功の場合はTrue
        """
        thread_id = threading.current_thread().ident

        if not params_list:
            self.logger.warning(
                f"スレッド {thread_id}: パラメータリストが空のため executemany をスキップします"
            )
            return True

        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            cursor.executemany(query, params_list)

            if not self.db._transaction_active:
                conn.commit()

            self.logger.debug(
                f"スレッド {thread_id}: executemanyを実行しました（{len(params_list)}件）: {query[:100]}..."
            )
            return True

        except Exception as e:
            self.logger.error(f"スレッド {thread_id}: executemany実行エラー: {e}")
            self.logger.debug(f"実行しようとしたクエリ: {query}")
            self.logger.debug(f"パラメータ数: {len(params_list)}")

            if not self.db._transaction_active:
                conn.rollback()

            return False

    def fetchone(self, query, params=None):
        """
        クエリを実行して1行の結果を取得する

        Args:
            query (str): 実行するSQLクエリ
            params (tuple, list, dict, optional): クエリのパラメータ

        Returns:
            tuple: 1行の結果（結果がない場合はNone）
        """
        thread_id = threading.current_thread().ident

        try:
            conn = self.db.connect()
            cursor = conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            row = cursor.fetchone()

            self.logger.debug(
                f"スレッド {thread_id}: fetchoneを実行しました: {query[:100]}..."
            )
            return row

        except Exception as e:
            self.logger.error(f"スレッド {thread_id}: fetchone実行エラー: {e}")
            self.logger.debug(f"実行しようとしたクエリ: {query}")
            if params:
                self.logger.debug(f"パラメータ: {params}")

            return None

    def fetchall(self, query, params=None):
        """
        クエリを実行して全ての結果を取得する

        Args:
            query (str): 実行するSQLクエリ
            params (tuple, list, dict, optional): クエリのパラメータ

        Returns:
            list: 結果の行のリスト
        """
        thread_id = threading.current_thread().ident

        try:
            conn = self.db.connect()
            cursor = conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            rows = cursor.fetchall()

            self.logger.debug(
                f"スレッド {thread_id}: fetchallを実行しました（{len(rows)}件）: {query[:100]}..."
            )
            return rows

        except Exception as e:
            self.logger.error(f"スレッド {thread_id}: fetchall実行エラー: {e}")
            self.logger.debug(f"実行しようとしたクエリ: {query}")
            if params:
                self.logger.debug(f"パラメータ: {params}")

            return []

    def query_to_dataframe(self, query, params=None, columns=None):
        """
        クエリを実行して結果をDataFrameとして取得する

        Args:
            query (str): 実行するSQLクエリ
            params (tuple, list, dict, optional): クエリのパラメータ
            columns (list, optional): 列名のリスト（省略時はカーソル記述から取得）

        Returns:
            DataFrame: 結果のDataFrame
        """
        thread_id = threading.current_thread().ident

        try:
            conn = self.db.connect()

            if params:
                df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)

            # 列名が指定されている場合は上書き
            if columns and len(columns) == len(df.columns):
                df.columns = columns

            self.logger.debug(
                f"スレッド {thread_id}: query_to_dataframeを実行しました（{len(df)}行, {len(df.columns)}列）"
            )
            return df

        except Exception as e:
            self.logger.error(
                f"スレッド {thread_id}: query_to_dataframe実行エラー: {e}"
            )
            self.logger.debug(f"実行しようとしたクエリ: {query}")
            if params:
                self.logger.debug(f"パラメータ: {params}")

            # 空のDataFrameを返す（列名が指定されている場合はその列を持つ空DF）
            if columns:
                return pd.DataFrame(columns=columns)
            else:
                return pd.DataFrame()
