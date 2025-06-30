"""
データベーステーブル管理の基本機能を提供するモジュール
"""

import logging


class TableManager:
    """
    データベーステーブルを管理するクラス
    """

    def __init__(self, db_connector, query_executor, logger=None):
        """
        初期化

        Args:
            db_connector: データベース接続オブジェクト
            query_executor: クエリ実行オブジェクト
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.db = db_connector
        self.query = query_executor
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("TableManagerが初期化されました")

    def table_exists(self, table_name):
        """
        テーブルが存在するか確認する

        Args:
            table_name (str): テーブル名

        Returns:
            bool: テーブルが存在する場合はTrue
        """
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE() 
        AND table_name = %s
        """
        result = self.query.fetchone(query, (table_name,))
        return result is not None

    def create_table(self, table_name, schema, primary_key=None, if_not_exists=True):
        """
        テーブルを作成する

        Args:
            table_name (str): テーブル名
            schema (dict): カラム名と型の辞書
            primary_key (str or list, optional): プライマリキーのカラム名（複合キーの場合はリスト）
            if_not_exists (bool): テーブルが存在しない場合のみ作成するかどうか

        Returns:
            bool: 作成成功の場合はTrue
        """
        # スキーマからSQLを構築
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")

        # プライマリキーの設定
        if primary_key:
            if isinstance(primary_key, list):
                pk_clause = f", PRIMARY KEY ({', '.join(primary_key)})"
            else:
                pk_clause = f", PRIMARY KEY ({primary_key})"
        else:
            pk_clause = ""

        # テーブル作成SQL
        not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        columns_clause = ", ".join(columns)
        create_sql = """
        CREATE TABLE {not_exists} {table} (
            {cols}{pk}
        )
        """.format(
            not_exists=not_exists_clause,
            table=table_name,
            cols=columns_clause,
            pk=pk_clause,
        )

        success = self.query.execute(create_sql)

        if success:
            self.logger.info(f"テーブル {table_name} を作成しました")
        else:
            self.logger.error(f"テーブル {table_name} の作成に失敗しました")

        return success

    def drop_table(self, table_name, if_exists=True):
        """
        テーブルを削除する

        Args:
            table_name (str): テーブル名
            if_exists (bool): テーブルが存在する場合のみ削除するかどうか

        Returns:
            bool: 削除成功の場合はTrue
        """
        # 存在する場合のみ句
        if_exists_clause = "IF EXISTS " if if_exists else ""

        drop_sql = """
        DROP TABLE {exists} {table}
        """.format(
            exists=if_exists_clause, table=table_name
        )

        success = self.query.execute(drop_sql)

        if success:
            self.logger.info(f"テーブル {table_name} を削除しました")
        else:
            self.logger.error(f"テーブル {table_name} の削除に失敗しました")

        return success

    def truncate_table(self, table_name):
        """
        テーブルの全データを削除する

        Args:
            table_name (str): テーブル名

        Returns:
            bool: 削除成功の場合はTrue
        """
        if not self.table_exists(table_name):
            self.logger.warning(
                f"テーブル {table_name} は存在しないため truncate はスキップします"
            )
            return True

        truncate_sql = f"DELETE FROM {table_name}"

        success = self.query.execute(truncate_sql)

        if success:
            self.logger.info(f"テーブル {table_name} のデータを全て削除しました")
        else:
            self.logger.error(f"テーブル {table_name} のデータ削除に失敗しました")

        return success

    def add_column(self, table_name, column_name, column_type):
        """
        テーブルに列を追加する

        Args:
            table_name (str): テーブル名
            column_name (str): 追加する列名
            column_type (str): 列の型

        Returns:
            bool: 追加成功の場合はTrue
        """
        if not self.table_exists(table_name):
            self.logger.error(f"テーブル {table_name} は存在しないため列追加できません")
            return False

        # 列が既に存在するか確認
        column_info_query = f"PRAGMA table_info({table_name})"
        columns = self.query.fetchall(column_info_query)

        # 列名のリストを取得
        column_names = [col[1] for col in columns]

        if column_name in column_names:
            self.logger.warning(f"列 {column_name} は既に {table_name} に存在します")
            return True

        # 列を追加
        alter_sql = """
        ALTER TABLE {table} 
        ADD COLUMN {col} {type}
        """.format(
            table=table_name, col=column_name, type=column_type
        )

        success = self.query.execute(alter_sql)

        if success:
            self.logger.info(
                f"テーブル {table_name} に列 {column_name} ({column_type}) を追加しました"
            )
        else:
            self.logger.error(
                f"テーブル {table_name} への列 {column_name} の追加に失敗しました"
            )

        return success

    def get_table_schema(self, table_name):
        """
        テーブルのスキーマ情報を取得する

        Args:
            table_name (str): テーブル名

        Returns:
            list: 列情報のリスト
        """
        if not self.table_exists(table_name):
            self.logger.error(
                f"テーブル {table_name} は存在しないためスキーマを取得できません"
            )
            return []

        schema_query = f"PRAGMA table_info({table_name})"
        return self.query.fetchall(schema_query)

    def insert_dataframe(self, df, table_name, if_exists="append"):
        """
        DataFrameをテーブルに挿入する

        Args:
            df (DataFrame): 挿入するデータフレーム
            table_name (str): テーブル名
            if_exists (str): テーブルが存在する場合の動作 ('fail', 'replace', 'append')

        Returns:
            bool: 挿入成功の場合はTrue
        """
        if df.empty:
            self.logger.warning(
                f"空のDataFrameのため {table_name} への挿入をスキップします"
            )
            return True

        try:
            conn = self.db.connect()
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)

            self.logger.info(f"{len(df)} 行のデータを {table_name} に挿入しました")
            return True

        except Exception as e:
            self.logger.error(f"DataFrameの {table_name} への挿入に失敗: {e}")
            return False

    def upsert_dataframe(self, df, table_name, key_columns):
        """
        DataFrameを挿入または更新する（一意のキーに基づく）

        Args:
            df (DataFrame): 挿入/更新するデータフレーム
            table_name (str): テーブル名
            key_columns (list): 一意キーとなる列名のリスト

        Returns:
            bool: 処理成功の場合はTrue
        """
        if df.empty:
            self.logger.warning(
                f"空のDataFrameのため {table_name} へのupsertをスキップします"
            )
            return True

        # テーブルが存在しない場合は作成
        if not self.table_exists(table_name):
            self.logger.info(f"テーブル {table_name} が存在しないため新規作成します")

            # DataFrameからスキーマを作成
            schema = {}
            for col, dtype in zip(df.columns, df.dtypes):
                if "int" in str(dtype):
                    schema[col] = "INTEGER"
                elif "float" in str(dtype):
                    schema[col] = "REAL"
                elif "bool" in str(dtype):
                    schema[col] = "BOOLEAN"
                else:
                    schema[col] = "TEXT"

            # テーブル作成
            self.create_table(table_name, schema, primary_key=key_columns)

            # 単純挿入
            return self.insert_dataframe(df, table_name)

        try:
            # 非キー列を取得
            non_key_columns = [col for col in df.columns if col not in key_columns]

            # 一時テーブル名
            temp_table = f"temp_{table_name}"

            # 一時テーブルにデータを挿入
            conn = self.db.connect()
            df.to_sql(temp_table, conn, if_exists="replace", index=False)

            # UPSERTクエリの構築
            update_cols = ", ".join(
                [f"{col}=excluded.{col}" for col in non_key_columns]
            )

            columns_str = ", ".join(df.columns)
            key_columns_str = ", ".join(key_columns)
            query = """
            INSERT INTO {table} ({columns})
            SELECT {columns} FROM {temp}
            ON CONFLICT ({keys}) 
            DO UPDATE SET {updates}
            """.format(
                table=table_name,
                columns=columns_str,
                temp=temp_table,
                keys=key_columns_str,
                updates=update_cols,
            )

            # UPSERTの実行
            success = self.query.execute(query)

            # 一時テーブルの削除
            drop_temp_query = "DROP TABLE {temp}".format(temp=temp_table)
            self.query.execute(drop_temp_query)

            if success:
                self.logger.info(
                    f"{len(df)} 行のデータを {table_name} にupsertしました"
                )

            return success

        except Exception as e:
            self.logger.error(f"DataFrameの {table_name} へのupsertに失敗: {e}")
            return False
