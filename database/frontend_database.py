import logging

import pandas as pd

import duckdb


class FrontendDatabase:
    """
    フロントエンド用のDuckDBデータベースを操作するクラス
    """

    def __init__(self, db_path: str, logger: logging.Logger = None):
        """
        コンストラクタ

        Args:
            db_path (str): DuckDBデータベースファイルのパス
            logger (logging.Logger, optional): ロガーインスタンス. Defaults to None.
        """
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self.conn = None

    def _connect(self):
        """データベースに接続する"""
        if (
            self.conn is None
        ):  # DuckDBは接続がスレッドセーフではないため、都度接続が推奨される場合がある
            try:
                # read_only=False で書き込み可能に
                self.conn = duckdb.connect(database=self.db_path, read_only=False)
                self.logger.debug(f"DuckDBに接続しました: {self.db_path}")
            except Exception as e:
                # ★ エラー詳細をより詳しくログ出力 ★
                error_type = type(e).__name__
                error_msg = str(e)
                self.logger.error(
                    f"DuckDBへの接続に失敗しました: Type={error_type}, Msg='{error_msg}'"
                )
                # スタックトレースも出力
                self.logger.exception("接続エラーのスタックトレース:")
                raise  # 接続失敗は致命的なので例外を再送出

    def _disconnect(self):
        """データベース接続を切断する"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.debug("DuckDB接続を切断しました")

    def create_table_from_dataframe(
        self, table_name: str, df: pd.DataFrame, if_exists: str = "replace"
    ):
        """
        Pandas DataFrameからテーブルを作成または置換/追記する

        Args:
            table_name (str): 作成するテーブル名
            df (pd.DataFrame): テーブルの元になるDataFrame
            if_exists (str, optional): テーブルが既に存在する場合の動作 ('fail', 'replace', 'append').
                                       Defaults to 'replace'.

        Returns:
            bool: 処理が成功したかどうか
        """
        if df.empty:
            self.logger.warning(
                f"DataFrameが空のため、テーブル '{table_name}' の作成/更新をスキップします。"
            )
            return True  # 空のDFはエラーではない

        self._connect()
        if not self.conn:
            self.logger.error(
                "DuckDBに接続されていないため、テーブルを作成できません。"
            )
            return False

        try:
            # DuckDBの register を使うと、DataFrameを一時的なビューとして登録できる
            # これをクエリ内で参照する
            self.conn.register("df_view", df)

            if if_exists == "replace":
                self.conn.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_view"
                )
                self.logger.info(
                    f"テーブル '{table_name}' を作成または置換しました ({len(df)}行)。"
                )
            elif if_exists == "append":
                # 追記の場合はテーブルが存在するか確認し、なければ作成する
                try:
                    # 既存テーブルの存在確認とスキーマ互換性の確認が必要な場合がある
                    # ここでは単純に追記を試みる
                    self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_view")
                    self.logger.info(
                        f"テーブル '{table_name}' にデータを追記しました ({len(df)}行)。"
                    )
                except duckdb.CatalogException:  # テーブルが存在しない場合
                    self.logger.info(
                        f"テーブル '{table_name}' が存在しないため、新規作成します。"
                    )
                    self.conn.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM df_view"
                    )
                    self.logger.info(
                        f"テーブル '{table_name}' を新規作成しました ({len(df)}行)。"
                    )
            elif if_exists == "fail":
                # テーブルが存在しないこと確認してから作成
                try:
                    self.conn.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM df_view"
                    )
                    self.logger.info(
                        f"テーブル '{table_name}' を新規作成しました ({len(df)}行)。"
                    )
                except duckdb.CatalogException:  # テーブルが既に存在する場合
                    self.logger.error(
                        f"テーブル '{table_name}' は既に存在します (if_exists='fail')。"
                    )
                    return False
            else:
                self.logger.error(f"不明な if_exists オプション: {if_exists}")
                return False

            # 一時ビューを解除
            self.conn.unregister("df_view")
            return True

        except Exception as e:
            # ★ エラー詳細をより詳しくログ出力 ★
            error_type = type(e).__name__
            error_msg = str(e)
            self.logger.error(
                f"テーブル '{table_name}' の作成/更新中にエラーが発生しました: Type={error_type}, Msg='{error_msg}'"
            )
            # スタックトレースも出力
            self.logger.exception(
                f"テーブル '{table_name}' の作成/更新エラーのスタックトレース:"
            )
            # エラーが発生した場合でもビューを解除しようと試みる
            try:
                if (
                    self.conn
                    and "df_view"
                    in self.conn.execute("SHOW TABLES").df()["name"].tolist()
                ):
                    self.conn.unregister("df_view")
            except Exception as unregister_e:
                self.logger.error(
                    f"一時ビュー 'df_view' の解除中にエラー: {unregister_e}"
                )
            return False
        finally:
            self._disconnect()

    def execute_query(self, query: str) -> pd.DataFrame | None:
        """
        SELECTクエリを実行し、結果をDataFrameで返す

        Args:
            query (str): 実行するSQLクエリ

        Returns:
            pd.DataFrame | None: クエリ結果のDataFrame、またはエラー時にNone
        """
        self._connect()
        if not self.conn:
            self.logger.error("DuckDBに接続されていないため、クエリを実行できません。")
            return None
        try:
            result_df = self.conn.execute(query).fetchdf()
            self.logger.debug(
                f"クエリを実行しました: {query[:100]}..."
            )  # クエリが長い場合省略
            return result_df
        except Exception as e:
            self.logger.error(f"クエリ実行中にエラーが発生しました: {query} - {e}")
            return None
        finally:
            self._disconnect()

    # 必要に応じて他のメソッド (テーブル一覧取得、テーブル削除など) を追加
