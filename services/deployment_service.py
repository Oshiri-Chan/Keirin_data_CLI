import logging

# 型ヒントのためにインポートするが、循環参照を避けるため TYPE_CHECKING を利用
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from database.frontend_database import FrontendDatabase
    from database.keirin_database import KeirinDatabase


class DeploymentService:
    """
    MySQLデータベースからDuckDBデータベースへデータをデプロイするサービス

    このモジュールは、MySQLで管理されている競輪データをDuckDBフォーマットに
    変換してフロントエンド用のデータファイルを生成する機能を提供します。
    """

    def __init__(
        self,
        source_db: "KeirinDatabase",
        target_db: "FrontendDatabase",
        logger: logging.Logger,
    ):
        """
        コンストラクタ

        Args:
            source_db (KeirinDatabase): データソースとなるMySQLデータベースインスタンス
            target_db (FrontendDatabase): デプロイ先となるDuckDBデータベースインスタンス
            logger (logging.Logger): ロガーインスタンス
        """
        self.source_db = source_db
        self.target_db = target_db
        self.logger = logger

    def get_source_tables(self) -> list[str]:
        """
        データソース(MySQL)からテーブル名のリストを取得する。
        KeirinDatabaseクラスにテーブル一覧を取得するメソッドが必要。
        ここでは仮のメソッド名 get_all_table_names() を使用する。

        Returns:
            list[str]: テーブル名のリスト
        """
        try:
            # KeirinDatabaseクラスに get_all_table_names のようなメソッドがあると仮定
            # もし無ければ、'sqlite_master'テーブルをクエリするなどして実装する必要がある
            if hasattr(self.source_db, "get_all_table_names") and callable(
                self.source_db.get_all_table_names
            ):
                tables = self.source_db.get_all_table_names()
                self.logger.info(f"ソースDBからテーブルリストを取得しました: {tables}")
                return tables
            else:
                # get_all_table_names がない場合のフォールバック (information_schemaから取得)
                self.logger.warning(
                    "'get_all_table_names' メソッドが見つかりません。information_schemaから取得します。"
                )
                query = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE';"
                df_tables = self.source_db.read_from_mysql(
                    query=query
                )  # read_from_mysql が汎用クエリを受け付ける必要がある
                if df_tables is not None and not df_tables.empty:
                    tables = df_tables["table_name"].tolist()
                    self.logger.info(
                        f"ソースDBからテーブルリストを取得しました (information_schema): {tables}"
                    )
                    return tables
                else:
                    self.logger.error(
                        "information_schemaからのテーブルリスト取得に失敗しました。"
                    )
                    return []
        except Exception as e:
            self.logger.error(f"ソースDBからのテーブルリスト取得中にエラー: {e}")
            return []

    def deploy_table(self, table_name: str) -> bool:
        """
        指定されたテーブルをMySQLからDuckDBへデプロイする

        Args:
            table_name (str): デプロイするテーブル名

        Returns:
            bool: デプロイが成功したかどうか
        """
        self.logger.info(f"テーブル '{table_name}' のデプロイを開始します...")
        try:
            # 1. MySQLからデータをDataFrameとして読み込み
            # KeirinDatabase.read_from_sqlite がテーブル名を引数に取るか確認
            # ★ query=query ではなく table_name=table_name を渡すように修正 ★
            # query = f"SELECT * FROM {table_name}" # この行は不要になる
            df = self.source_db.read_from_mysql(table_name=table_name)

            if df is None:
                self.logger.error(f"テーブル '{table_name}' の読み込みに失敗しました。")
                return False

            if df.empty:
                self.logger.warning(
                    f"テーブル '{table_name}' は空です。デプロイは成功として扱います。"
                )
                # 空のテーブルもDuckDB側に（空で）作成または置換する
                # FrontendDatabase の create_table_from_dataframe は空のDFを扱えるように実装済み
                success = self.target_db.create_table_from_dataframe(
                    table_name, df, if_exists="replace"
                )
                return success

            # 2. DuckDBにDataFrameを書き込み (既存データは置換する)
            success = self.target_db.create_table_from_dataframe(
                table_name, df, if_exists="replace"
            )

            if success:
                self.logger.info(
                    f"テーブル '{table_name}' のデプロイが完了しました。 ({len(df)}行)"
                )
            else:
                self.logger.error(
                    f"テーブル '{table_name}' のDuckDBへの書き込みに失敗しました。"
                )

            return success

        except Exception as e:
            self.logger.error(
                f"テーブル '{table_name}' のデプロイ中にエラーが発生しました: {e}"
            )
            return False

    def deploy_all_tables(self) -> bool:
        """
        ソースデータベースの全てのテーブルをデプロイする

        Returns:
            bool: 全てのテーブルのデプロイが成功したかどうか
        """
        self.logger.info("全てのテーブルのデプロイ処理を開始します...")
        tables_to_deploy = self.get_source_tables()

        if not tables_to_deploy:
            self.logger.warning("デプロイ対象のテーブルが見つかりませんでした。")
            return True  # 対象がない場合は成功とする

        overall_success = True
        for table_name in tables_to_deploy:
            success = self.deploy_table(table_name)
            if not success:
                overall_success = False
                # エラーが発生しても処理を続けるか、ここで中断するかは要件次第
                # self.logger.error(f"テーブル '{table_name}' のデプロイに失敗したため、処理を中断します。")
                # break # 中断する場合

        if overall_success:
            self.logger.info("全てのテーブルのデプロイ処理が正常に完了しました。")
        else:
            self.logger.warning(
                "一部のテーブルのデプロイに失敗しました。ログを確認してください。"
            )

        return overall_success
