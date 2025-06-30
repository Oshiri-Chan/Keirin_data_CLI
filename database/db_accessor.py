"""
データベースアクセサーモジュール
"""

import configparser
import logging
import os
import time
from datetime import date, datetime

# from threading import Lock # Lock は現在未使用のためコメントアウト
from typing import Any, Dict, List, Optional, Union, Callable

import mysql.connector
from mysql.connector import pooling  # 接続プーリングのために追加


class KeirinDataAccessor:
    """
    競輪データベースとのアクセスを担当するクラス (MySQL専用)
    データの取得や保存のロジックを提供
    """

    def __init__(
        self,
        config_path="config/config.ini",
        logger: logging.Logger = None,
    ):
        """
        初期化

        Args:
            config_path (str): 設定ファイルのパス
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.logger = logger or logging.getLogger(__name__)
        # self._lock = Lock() # 新しい接続戦略では不要になる可能性
        self.mysql_config = self._load_mysql_config(config_path)
        if not self.mysql_config:
            self.logger.error("MySQL設定のロードに失敗しました。処理を続行できません。")
            raise ValueError("MySQL設定のロード失敗")

        # 接続プールの設定
        self.pool_name = self.mysql_config.pop("pool_name", "keirin_pool")
        self.pool_size = int(
            self.mysql_config.pop("pool_size", 5)
        )  # configからpool_sizeを取得、なければデフォルト5

        # 一時的に接続プールを無効化（ハング問題の修正まで）
        self.use_connection_pool = False

        if self.use_connection_pool:
            try:
                # 接続タイムアウトを追加
                connection_config = {
                    **self.mysql_config,
                    "connection_timeout": 10,  # 10秒でタイムアウト
                    "autocommit": True,
                }

                self.cnxpool = pooling.MySQLConnectionPool(
                    pool_name=self.pool_name,
                    pool_size=self.pool_size,
                    **connection_config,
                )
                self.logger.info(
                    f"MySQL接続プール '{self.pool_name}' (サイズ:{self.pool_size}) を作成しました。"
                )
            except mysql.connector.Error as err:
                self.logger.error(
                    f"MySQL接続プールの作成に失敗しました: {err}", exc_info=True
                )
                raise
        else:
            self.cnxpool = None
            self.logger.info(
                "MySQL接続プールは一時的に無効化されています。直接接続を使用します。"
            )

        # デッドロック/ロックタイムアウトのエラーコード
        self.DEADLOCK_ERROR_CODES = (
            1213,
            1205,
        )  # 1213: Deadlock, 1205: Lock wait timeout
        self.MAX_RETRY_ATTEMPTS = 3
        self.RETRY_DELAY_BASE = 0.5  # 基本待機時間（秒）

        # ロック順序設定の読み込み
        self.lock_order = self._load_lock_order_config(config_path)

    def _load_lock_order_config(self, main_config_path: str) -> List[str]:
        """deadrock.iniからロック順序設定を読み込む"""
        config_dir = os.path.dirname(main_config_path)
        lock_order_config_path = os.path.join(config_dir, "deadrock.ini")

        parser = configparser.ConfigParser()
        if not os.path.exists(lock_order_config_path):
            self.logger.warning(
                f"ロック順序設定ファイルが見つかりません: {lock_order_config_path}。"
                "ロック順序は強制されません。"
            )
            return []

        try:
            # encodingをutf-8-sigからutf-8へ変更（BOMなしを想定）
            with open(lock_order_config_path, "r", encoding="utf-8") as f:
                parser.read_file(f)
        except Exception as e:
            self.logger.error(
                f"ロック順序設定ファイルの読み込み中にエラー: {lock_order_config_path}, error: {e}",
                exc_info=True,
            )
            return []

        if "LockOrder" in parser and "order" in parser["LockOrder"]:
            order_str = parser["LockOrder"]["order"]
            # 空白や改行を考慮してsplitし、各要素をstrip
            lock_order_list = [
                table_name.strip()
                for table_name in order_str.split(",")
                if table_name.strip()
            ]
            if lock_order_list:
                self.logger.info(
                    f"ロック順序をロードしました: {lock_order_list} (from {lock_order_config_path})"
                )
                return lock_order_list
            else:
                self.logger.warning(
                    f"ロック順序設定ファイル ({lock_order_config_path}) の 'order' が空か、"
                    "パース後に有効なテーブル名がありませんでした。"
                )
                return []
        else:
            self.logger.warning(
                f"ロック順序設定ファイル ({lock_order_config_path}) に "
                "[LockOrder] セクションまたは 'order' キーが見つかりません。"
            )
            return []

    def _load_mysql_config(self, config_path: str) -> Optional[Dict[str, str]]:
        """config.iniからMySQL設定を読み込む"""
        parser = configparser.ConfigParser()

        if not os.path.isabs(config_path):
            abs_config_path = os.path.abspath(config_path)
        else:
            abs_config_path = config_path

        if not os.path.exists(abs_config_path):
            self.logger.error(f"設定ファイルが見つかりません: {abs_config_path}")
            # raise FileNotFoundError(f"設定ファイルが見つかりません: {abs_config_path}") # 元のコード
            # config.iniにpool_nameとpool_sizeを追記することを推奨するエラーメッセージに変更
            raise FileNotFoundError(
                f"設定ファイルが見つかりません: {abs_config_path}. "
                f"config.iniの[MySQL]セクションに 'pool_name' と 'pool_size' (推奨値: 5-10) を追加してください。"
            )

        try:
            with open(
                abs_config_path,
                "r",
                encoding="utf-8-sig",
            ) as f:
                parser.read_file(f)
        except Exception as e:
            self.logger.error(
                f"設定ファイルの読み込み中にエラーが発生しました: "
                f"{abs_config_path}, error: {e}"
            )
            raise

        if "MySQL" in parser:
            config_details = dict(parser["MySQL"])
            if "port" in config_details:
                try:
                    config_details["port"] = int(config_details["port"])
                except ValueError:
                    self.logger.warning(
                        f"MySQLのport設定 '{config_details['port']}' は不正な数値です。デフォルトの3306を使用します。"
                    )
                    config_details["port"] = 3306
            else:
                config_details["port"] = 3306
            return config_details
        else:
            self.logger.error("MySQLの設定がconfig.iniに見つかりません。")
            raise ValueError("MySQLの設定がconfig.iniに見つかりません。")

    def _get_new_connection(self) -> mysql.connector.connection.MySQLConnection:
        """
        新しいMySQLデータベース接続を確立して返す。
        """
        try:
            if self.use_connection_pool and self.cnxpool:
                self.logger.debug(
                    f"MySQL接続プール '{self.pool_name}' から接続を取得します。"
                )
                conn = self.cnxpool.get_connection()
                self.logger.info(
                    f"MySQL接続プール '{self.pool_name}' から接続を取得しました。 (接続ID: {conn.connection_id})"
                )
                return conn
            else:
                # 直接接続を作成（初期化時に読み込んだ設定を使用）
                self.logger.debug("MySQL直接接続を作成します。")

                # 初期化時に読み込んだ設定を使用（simple_mysql_test.pyと同じパラメータ）
                config_for_direct = {
                    **self.mysql_config,  # 初期化時に読み込んだ設定を使用
                    "charset": "utf8mb4",
                    "collation": "utf8mb4_unicode_ci",
                }

                conn = mysql.connector.connect(**config_for_direct)
                self.logger.info(
                    f"MySQL直接接続を作成しました。 (接続ID: {conn.connection_id})"
                )
                return conn
        except mysql.connector.Error as err:
            self.logger.error(f"MySQL接続取得エラー: {err}", exc_info=True)
            raise

    def _execute_with_retry(
        self, method: Callable, *args, max_retries: Optional[int] = None, **kwargs
    ) -> Any:
        """
        デッドロックやロックタイムアウトに対するリトライ機構

        Args:
            method: 実行するメソッド
            max_retries: 最大リトライ回数. Noneの場合はself.MAX_RETRY_ATTEMPTSを使用

        Returns:
            メソッドの実行結果

        Raises:
            mysql.connector.Error: リトライ回数を超えてもエラーが解消しない場合
        """
        actual_max_retries = (
            max_retries if max_retries is not None else self.MAX_RETRY_ATTEMPTS
        )
        last_error = None
        for attempt in range(actual_max_retries):
            try:
                return method(*args, **kwargs)
            except mysql.connector.Error as e:
                last_error = e
                # デッドロック (1213) またはロック待機タイムアウト (1205)
                if e.errno in self.DEADLOCK_ERROR_CODES:
                    if attempt < actual_max_retries - 1:
                        wait_time = (attempt + 1) * 0.5  # 線形バックオフ
                        self.logger.warning(
                            f"デッドロック検出。{wait_time}秒後にリトライします。"
                            f"(試行 {attempt + 1}/{actual_max_retries})"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(
                            f"最大リトライ回数 ({actual_max_retries}) に達しました。"
                            f"エラー: {e}"
                        )
                else:
                    # その他のMySQLエラーは即座に再スロー
                    self.logger.error(f"MySQL実行エラー (リトライ対象外): {e}")
                raise
            except Exception as e:
                # MySQL以外のエラーは即座に再スロー
                self.logger.error(f"予期しないエラー: {e}")
                raise

        # 最後のエラーを再スロー
        if last_error:
            raise last_error

    def __enter__(self):
        """コンテキストマネージャのエントリーポイント。"""
        # トランザクション管理は行わないため、単にselfを返す
        # 以前のトランザクション関連の警告ログも削除
        self.logger.debug(
            "KeirinDataAccessorがコンテキストマネージャとして使われました（トランザクション管理は行いません）。"
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャの終了ポイント。"""
        # トランザクション管理は行わないため、特別な処理は不要
        # 以前のコミット/ロールバック処理、一時接続のクローズ処理は削除
        self.logger.debug(
            "KeirinDataAccessorのコンテキストマネージャのスコープを抜けました（トランザクション管理は行いません）。"
        )
        # 例外が発生した場合でも、ここでは特に何も処理しない (呼び出し元で処理される想定)
        return False  # 例外が発生した場合、それを再送出する

    def _close_connection_resources(
        self,
        close_cursor_only: bool = False,
        force_close_conn: bool = False,
    ):
        """カーソルや接続を閉じる内部メソッド"""
        if self.cursor:
            try:
                self.cursor.close()
                self.logger.debug("カーソルをクローズしました。")
            except mysql.connector.Error as e:
                self.logger.warning(
                    f"カーソルのクローズ中にエラー: {e}",
                    exc_info=True,
                )
            finally:
                self.cursor = None

        if not close_cursor_only or force_close_conn:
            if self.conn and self.conn.is_connected():
                try:
                    self.conn.close()
                    self.logger.info("データベース接続をクローズしました。")
                except mysql.connector.Error as e:
                    self.logger.warning(
                        f"データベース接続のクローズ中にエラー: {e}",
                        exc_info=True,
                    )
                finally:
                    self.conn = None

    def execute_query(
        self,
        query: str,
        params: Union[tuple, Dict[str, Any]] = None,
        fetch_one: bool = False,
        dictionary: bool = True,
        # 新規追加: 既存の接続とカーソルを使用するオプション
        existing_conn: Optional[mysql.connector.connection.MySQLConnection] = None,
        existing_cursor: Optional[mysql.connector.cursor.MySQLCursor] = None,
    ) -> Any:
        """
        SQLクエリを実行し、結果を返す (MySQL用)。
        このメソッド内で新しい接続とカーソルを作成・クローズする。
        デッドロック発生時は自動的にリトライする。
        """

        def _execute():
            results = None
            conn = None
            cursor = None
            try:
                # 既存の接続とカーソルを使用する場合
                if existing_conn and existing_cursor:
                    conn = existing_conn
                    cursor = existing_cursor
                else:  # 新しい接続を取得
                    conn = self._get_new_connection()
                    cursor = conn.cursor(dictionary=dictionary, buffered=True)

                self.logger.debug(
                    f"クエリ実行 (新規接続): {query}, パラメータ: {params}"
                )
                cursor.execute(query, params)

                if cursor.with_rows:  # 結果セットがあるクエリの場合 (主にSELECT)
                    if fetch_one:
                        results = cursor.fetchone()
                    else:
                        results = cursor.fetchall()
                # INSERT/UPDATE/DELETE の場合は with_rows が False になることがある
                # その場合、cursor.rowcount などで影響行数を取得可能
                # ここでは結果セットの取得のみを主眼とする

                self.logger.debug(f"クエリ成功: {query}")
                return results
            except mysql.connector.Error as err:
                self.logger.error(
                    f"クエリエラー: {err} (クエリ: {query}, パラメータ: {params})",
                    exc_info=True,
                )
                if (
                    conn and conn.is_connected()
                ):  # エラー時もロールバックを試みる (autocommit=False のため)
                    try:
                        conn.rollback()
                        self.logger.debug("エラー発生のためロールバックしました。")
                    except mysql.connector.Error as rb_err:
                        self.logger.error(f"ロールバック中にエラー: {rb_err}")
                raise  # 元のエラーを再スロー
            finally:
                if cursor:
                    # existing_cursor が指定されていない場合のみクローズ
                    if not existing_cursor:
                        try:
                            cursor.close()
                        except mysql.connector.Error as e_cur:
                            self.logger.warning(
                                f"カーソルクローズエラー(execute_query): {e_cur}"
                            )
                # conn が存在し、かつ existing_conn でない場合（つまり、この関数内で新規に取得した接続の場合）のみクローズ
                if conn and not existing_conn and conn.is_connected():
                    try:
                        conn.close()
                        self.logger.debug(
                            "データベース接続(execute_query)をクローズしました。"
                        )
                    except mysql.connector.Error as e_conn:
                        self.logger.warning(
                            f"接続クローズエラー(execute_query): {e_conn}"
                        )

        # リトライ機構を使用して実行
        return self._execute_with_retry(_execute, max_retries=self.MAX_RETRY_ATTEMPTS)

    def execute_scalar(
        self,
        query: str,
        params: Union[tuple, Dict[str, Any]] = None,
    ) -> Any:
        """SQLクエリを実行し、単一の値を返す (MySQL用)"""
        result = self.execute_query(
            query,
            params,
            fetch_one=True,
            dictionary=False,
        )
        if result:
            return result[0]
        return None

    def execute_many(
        self,
        query: str,
        params_list: List[Union[tuple, Dict[str, Any]]],
        # 新規追加: 既存の接続とカーソルを使用するオプション
        existing_conn: Optional[mysql.connector.connection.MySQLConnection] = None,
        existing_cursor: Optional[mysql.connector.cursor.MySQLCursor] = None,
    ) -> int:
        """
        複数のSQLクエリを一括実行 (MySQL用)。
        このメソッド内で新しい接続とカーソルを作成・クローズし、トランザクションも管理する。
        デッドロック発生時は自動的にリトライする。
        """
        if not params_list:
            self.logger.info("execute_many: パラメータリストが空です。")
            return 0

        def _execute():
            conn = None
            cursor = None
            affected_rows = 0
            try:
                if existing_conn and existing_cursor:
                    conn = existing_conn
                    cursor = existing_cursor
                else:  # 新しい接続を取得
                    conn = self._get_new_connection()
                    cursor = (
                        conn.cursor()
                    )  # executemanyではdictionaryカーソルは通常不要

                self.logger.debug(
                    f"実行クエリ (many): {query}, パラメータ数: {len(params_list)}"
                )
                # パラメータリストが大きい場合は一部のみログ出力
                log_params_preview = params_list[:3]
                if len(params_list) > 3:
                    self.logger.debug(f"パラメータプレビュー: {log_params_preview}...")
                else:
                    self.logger.debug(f"パラメータ: {log_params_preview}")

                cursor.executemany(query, params_list)
                affected_rows = cursor.rowcount

                if not existing_conn:  # 既存の接続を使用していない場合のみコミット
                    conn.commit()
                    self.logger.info(
                        f"クエリ (many) 正常終了、{affected_rows}行影響。コミットしました。"
                    )
                else:  # トランザクションの一部として実行された場合
                    self.logger.info(
                        f"クエリ (many) 正常終了、{affected_rows}行影響。(トランザクション内、コミットは外部で管理)"
                    )

                return affected_rows

            except mysql.connector.Error as e:
                self.logger.error(
                    f"execute_manyエラー ({query[:100]}...): {e}", exc_info=True
                )
                if (
                    not existing_conn and conn
                ):  # エラー時かつ自動コミットモードならロールバック試行
                    try:
                        conn.rollback()
                        self.logger.warning(
                            "execute_manyエラーのためロールバックしました。"
                        )
                    except mysql.connector.Error as rb_err:
                        self.logger.error(f"ロールバック試行中エラー: {rb_err}")
                raise  # 元のエラーを再スロー
            finally:
                if cursor:
                    # existing_cursor が指定されていない場合のみクローズ
                    if not existing_cursor:
                        try:
                            cursor.close()
                        except mysql.connector.Error as e_cur:
                            self.logger.warning(
                                f"カーソルクローズエラー(execute_many): {e_cur}"
                            )
                # conn が存在し、かつ existing_conn でない場合のみクローズ
                if conn and not existing_conn and conn.is_connected():
                    try:
                        conn.close()
                        self.logger.debug(
                            "データベース接続(execute_many)をクローズしました。"
                        )
                    except mysql.connector.Error as e_conn:
                        self.logger.warning(
                            f"接続クローズエラー(execute_many): {e_conn}"
                        )

        # リトライ機構を使用して実行
        return self._execute_with_retry(_execute, max_retries=self.MAX_RETRY_ATTEMPTS)

    def execute_in_transaction(self, transaction_func, *args, **kwargs) -> Any:
        """
        トランザクション内で指定された関数を実行する。

        Args:
            transaction_func: トランザクション内で実行する関数
            *args, **kwargs: 関数に渡す引数

        Returns:
            関数の実行結果

        Raises:
            Exception: トランザクション内でエラーが発生した場合
        """
        conn = None
        try:
            conn = self._get_new_connection()
            # トランザクション開始 (明示的に autocommit を False に)
            # MySQL Connector/Pythonでは、デフォルトで autocommit は False だが、
            # プールされた接続の再利用時に状態がリセットされるか不明瞭なため、明示的に設定する方が安全。
            # ただし、conn.start_transaction() の方がより推奨される。
            # conn.autocommit = False # -> conn.start_transaction() へ
            conn.start_transaction()  # トランザクションを開始
            self.logger.debug(f"トランザクション開始 (接続ID: {conn.connection_id})")

            # 渡された関数を実行。connと、必要ならカーソルも渡せるようにする。
            # result = transaction_func(conn, conn.cursor(dictionary=True, buffered=True), *args, **kwargs)
            # transaction_func が conn と cursor を引数に取ることを前提とする。
            # Saver側の _atomic_xxx メソッドが conn, cursor を受け取るように修正済みなので、これで良い。

            # transaction_func のシグネチャに合わせて conn と cursor を渡す
            # cursor_for_tx = conn.cursor(dictionary=True, buffered=True) # 例: 辞書カーソル
            # result = transaction_func(conn, cursor_for_tx, *args, **kwargs)

            # transaction_func が (conn, cursor, *original_args, **original_kwargs) を期待すると仮定
            # そして、その内部で cursor.close() を行うと仮定
            result = transaction_func(
                conn, *args, **kwargs
            )  # conn のみを渡し、cursorは関数内で生成・管理

            conn.commit()
            self.logger.info(
                f"トランザクション正常終了、コミットしました (接続ID: {conn.connection_id})"
            )
            return result
        except mysql.connector.Error as e:
            self.logger.error(
                f"トランザクション中にMySQLエラー発生 (接続ID: {conn.connection_id if conn else 'N/A'}): {e}",
                exc_info=True,
            )
            if conn:
                try:
                    conn.rollback()
                    self.logger.warning(
                        f"エラーのためロールバックしました (接続ID: {conn.connection_id})"
                    )
                except mysql.connector.Error as rb_err:
                    self.logger.error(f"ロールバック試行中エラー: {rb_err}")
            raise  # 元のエラーを再スロー
        except Exception as e:  # MySQL以外のエラー
            self.logger.error(
                f"トランザクション中に予期せぬエラー発生 (接続ID: {conn.connection_id if conn else 'N/A'}): {e}",
                exc_info=True,
            )
            if conn:  # MySQL接続が開いていればロールバックを試みる
                try:
                    conn.rollback()
                    self.logger.warning(
                        f"予期せぬエラーのためロールバックしました (接続ID: {conn.connection_id})"
                    )
                except (
                    mysql.connector.Error
                ) as rb_err:  # ロールバック自体が失敗する可能性も考慮
                    self.logger.error(
                        f"ロールバック試行中エラー(予期せぬエラー後): {rb_err}"
                    )
            raise  # 元の例外を再送出
        finally:
            if conn:
                # 接続IDを先に保存（close後はアクセスできないため）
                try:
                    conn_id = conn.connection_id
                except Exception:
                    conn_id = "N/A"

                conn.close()  # 接続をプールに返却
                self.logger.debug(
                    f"トランザクション後、接続をプールに返却しました (接続ID: {conn_id})"
                )

    def get_winticket_races_to_update(
        self,
        venue_id: str,
        race_number: int,
        race_date: str,
        force_update_all: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Winticketのレース情報取得対象を判定 (MySQL版).
        - races テーブルに存在する
        - venues テーブルの venue_id (Winticket提供) と一致
        - races テーブルの race_number と一致
        - races テーブルの race_date (YYYY-MM-DD) と一致
        - force_update_all が True の場合は、ステータスに関わらず取得

        MySQLでは information_schema.tables を使用してテーブルの存在を確認できますが、
        ここでは races と venues テーブルは常に存在すると仮定します。

        race_date は races.start_at (BIGINT, Unixタイムスタンプ) から日付を比較する必要がある
        MySQLのFROM_UNIXTIMEとDATE関数を使用する
        """
        params = [venue_id, race_number, race_date]  # パラメータリストを初期化

        query_select_from = """
SELECT
    r.race_id,
    r.start_at,
    r.status AS race_status_api,
    rs.step3_status,
    rs.step4_status
    -- rs.step5_status -- step5_status が race_status に追加されたらコメント解除
FROM races r
JOIN cups c ON r.cup_id = c.cup_id
JOIN venues v ON c.venue_id = v.venue_id
LEFT JOIN race_status rs ON r.race_id = rs.race_id
"""
        where_conditions = [
            "v.venue_id = %s",
            "r.number = %s",
            "DATE(FROM_UNIXTIME(r.start_at)) = %s",
        ]

        if not force_update_all:
            status_conditions = [
                "(rs.step3_status != 'completed' OR rs.step3_status IS NULL)",
                "(rs.step4_status != 'completed' OR rs.step4_status IS NULL)",
                # "(rs.step5_status != 'completed' OR rs.step5_status IS NULL)" # step5_status が追加されたら
            ]
            where_conditions.append(f"({' OR '.join(status_conditions)})")

        query_where = "WHERE " + " AND ".join(where_conditions)
        query_order_by = "ORDER BY r.start_at ASC"

        final_query = query_select_from + "\n" + query_where + "\n" + query_order_by

        try:
            return self.execute_query(
                final_query,
                tuple(params),  # パラメータをタプルとして渡す
            )
        except mysql.connector.Error as e:
            error_log_message = (
                f"Winticketレース取得エラー (venue_id={venue_id}, "
                f"race_number={race_number}, race_date={race_date}): {e}"
            )
            self.logger.error(
                error_log_message,
                exc_info=True,
            )
            return []

    def get_yenjoy_races_to_update(
        self,
        force_update_all: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        YenJoyのオッズ更新または結果取得が必要なレースを取得します (MySQL版).
        - レースステータスが特定の値 (例: 発売中, 結果待ちなどAPIのステータス値による)
        - かつ, race_statusテーブルのstep3_status, step4_status, (将来的にstep5_status) が 'completed' でないもの
        - または, 最終オッズでない場合 (odds_statuses.final_odds = 0)
        - force_update_all が True の場合は、上記ステータスや final_odds の条件を無視する（ただし race.status の範囲は考慮）
        """
        # APIのレースステータス値 (races.status) で絞り込む必要がある
        # 例: 1 (発売前), 2 (発売中), 3 (締切), 4 (レース中), 5 (結果待), 6 (結果確定), 7 (中止), 8 (不成立)
        # 更新対象は、例えば 2, 3, 4, 5 あたりか。
        # YenJoyは主にオッズ(Step4)と結果(Step5)なので、races.status が 2以上6以下あたりが対象になりそう。
        # 厳密には、API仕様と更新ロジックに基づいて races.status の条件を決める。
        # ここでは仮に「まだ終わっていないレース」かつ「ステータスが未完了」を対象とする。

        query_select_from = """
SELECT
    r.race_id,
    r.start_at,
    r.status AS race_api_status,
    c.venue_id,
    v.venue_name,
    r.number AS race_number,
    DATE_FORMAT(FROM_UNIXTIME(r.start_at), '%Y%m%d') AS race_date_str,
    rs.step3_status,
    rs.step4_status,
    os.final_odds
    -- rs.step5_status -- race_statusに step5_status があれば
FROM races r
JOIN race_status rs ON r.race_id = rs.race_id
JOIN cups c ON r.cup_id = c.cup_id
JOIN venues v ON c.venue_id = v.venue_id
LEFT JOIN odds_statuses os ON r.race_id = os.race_id
"""
        where_conditions = [
            "(r.status BETWEEN 2 AND 5)"  # 基本的なレース状態の絞り込み
        ]

        if not force_update_all:
            status_and_odds_conditions = [
                "(rs.step3_status != 'completed' OR rs.step3_status IS NULL)",
                "(rs.step4_status != 'completed' OR rs.step4_status IS NULL)",
                # "(rs.step5_status != 'completed' OR rs.step5_status IS NULL)", -- step5_status が追加されたら
                "(os.final_odds = 0 OR os.final_odds IS NULL)",
            ]
            where_conditions.append(f"({' OR '.join(status_and_odds_conditions)})")

        query_where = "WHERE " + " AND ".join(where_conditions)
        query_order_by = "ORDER BY r.start_at ASC"

        final_query = query_select_from + "\n" + query_where + "\n" + query_order_by

        try:
            # パラメータがないので、そのまま final_query を渡す
            return self.execute_query(final_query)
        except mysql.connector.Error as e:
            error_log_message = f"YenJoy更新対象レース取得エラー: {e}"
            self.logger.error(
                error_log_message,
                exc_info=True,
            )
            return []

    def save_player_info(
        self,
        player_data_list: List[Dict[str, Any]],
    ):
        """
        選手情報をデータベースに一括保存/更新 (MySQL用).
        players テーブルに INSERT ... ON DUPLICATE KEY UPDATE を使用.
        """
        if not player_data_list:
            self.logger.info("保存する選手情報がありません。")
            return 0

        update_cols_template = [
            "name = VALUES(name)",
            "name_kana = VALUES(name_kana)",
            "birth_date = VALUES(birth_date)",
            "prefecture_id = VALUES(prefecture_id)",
            "term = VALUES(term)",
            "current_grade = VALUES(current_grade)",
        ]

        records_to_insert = []
        insert_column_names = []

        for player_data in player_data_list:
            record = {
                "player_id": player_data.get("id"),
                "name": player_data.get("name"),
                "name_kana": player_data.get("name_kana"),
                "term": player_data.get("term"),
                "current_grade": player_data.get("class"),
            }
            if "birthday" in player_data and player_data["birthday"]:
                try:
                    record["birth_date"] = player_data["birthday"]
                except ValueError:
                    log_message = (
                        f"選手の誕生日フォーマットエラー: {player_data.get('id')}, "
                        f"birthday: {player_data['birthday']}"
                    )
                    self.logger.warning(log_message)
                    record["birth_date"] = None

            record_clean = {k: v for k, v in record.items() if v is not None}

            if not insert_column_names and record_clean:
                insert_column_names = list(record_clean.keys())

            records_to_insert.append(record_clean)

        if not records_to_insert or not insert_column_names:
            self.logger.info("有効な選手情報がなかったため、保存をスキップします。")
            return 0

        insert_cols_str = ", ".join([f"`{col}`" for col in insert_column_names])
        placeholders_str = ", ".join(["%s"] * len(insert_column_names))
        update_str = ", ".join(
            [
                uc
                for uc in update_cols_template
                if uc.split(" = ")[0] in insert_column_names
                and uc.split(" = ")[0] != "player_id"
            ]
        )

        query = f"""
INSERT INTO players ({insert_cols_str})
VALUES ({placeholders_str})
ON DUPLICATE KEY UPDATE {update_str}
"""

        params_list_tuples = []
        for rec_dict in records_to_insert:
            params_list_tuples.append(
                tuple(rec_dict.get(col_name) for col_name in insert_column_names)
            )

        try:
            affected_rows = self.execute_many(
                query,
                params_list_tuples,
            )
            log_message = (
                f"選手情報を保存/更新しました ({len(params_list_tuples)}件試行、"
                f"影響行数: {affected_rows})"
            )
            self.logger.info(log_message)
            return affected_rows
        except mysql.connector.Error as e:
            log_message = f"選手情報の保存/更新エラー: {e}"
            self.logger.error(
                log_message,
                exc_info=True,
            )
            raise

    def _check_table_exists(self, table_name: str) -> bool:
        """MySQL用: テーブルが存在するか確認する."""
        query = """
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name = %s
"""
        try:
            count = self.execute_scalar(query, (table_name,))
            return count is not None and count > 0
        except mysql.connector.Error as e:
            log_message = f"テーブル存在確認エラー (テーブル名: {table_name}): {e}"
            self.logger.error(
                log_message,
                exc_info=True,
            )
            return False

    def get_yenjoy_races_to_update_for_step5(
        self,
        start_date_str: str,
        end_date_str: str,
        venue_codes: Optional[List[str]] = None,
        force_update_all: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Step5 (YenJoy結果取得) のために更新対象となるレース情報を取得する。
        """
        log_message = (
            f"Step5更新対象レース検索開始: {start_date_str} - {end_date_str}, "
            f"会場: {venue_codes or '全'}, 強制: {force_update_all}"
        )
        self.logger.info(log_message)

        params: List[Any] = []

        sql_select = """
            SELECT
                r.race_id,
                COALESCE(
                    DATE_FORMAT(FROM_UNIXTIME(r.start_at), '%Y-%m-%d'),
                    DATE_FORMAT(STR_TO_DATE(s.date, '%Y%m%d'), '%Y-%m-%d')
                ) AS race_date_db,
                c.venue_id AS venue_code,
                r.number AS race_number,
                s.date AS race_date_yyyymmdd, -- YenJoy URL用のレース日 (YYYYMMDD)
                DATE_FORMAT(c.start_date, '%Y%m%d') AS cup_start_date_yyyymmdd -- YenJoy URL用の開催初日 (YYYYMMDD)
            FROM races r
            JOIN schedules s ON r.schedule_id = s.schedule_id
            JOIN cups c ON s.cup_id = c.cup_id
        """

        sql_joins = ""
        sql_where_conditions: List[str] = []

        if not force_update_all:
            sql_joins += " JOIN race_status rs ON r.race_id = rs.race_id "
            sql_joins += " LEFT JOIN lap_data_status lds ON r.race_id = lds.race_id "

            sql_where_conditions.append("(lds.is_processed = 0 OR lds.race_id IS NULL)")

        try:
            datetime.strptime(
                start_date_str,
                "%Y-%m-%d",
            )
            datetime.strptime(end_date_str, "%Y-%m-%d")
            sql_where_conditions.append("DATE(FROM_UNIXTIME(r.start_at)) >= %s")
            params.append(start_date_str)
            sql_where_conditions.append("DATE(FROM_UNIXTIME(r.start_at)) <= %s")
            params.append(end_date_str)
        except ValueError:
            log_message = (
                f"日付フォーマットエラー: start_date='{start_date_str}', "
                f"end_date='{end_date_str}'。YYYY-MM-DD形式で指定してください。"
            )
            self.logger.error(log_message)
            return []

        if venue_codes:
            if not all(isinstance(vc, str) and vc.strip() for vc in venue_codes):
                log_message = (
                    f"不正な会場コードが含まれています: {venue_codes}。"
                    "会場コード条件は無視されます。"
                )
                self.logger.warning(log_message)
            else:
                venue_placeholders = ", ".join(["%s"] * len(venue_codes))
                sql_where_conditions.append(f"c.venue_id IN ({venue_placeholders})")
                params.extend(venue_codes)

        sql_order = " ORDER BY r.start_at, c.venue_id, r.number"

        final_sql = sql_select + sql_joins
        if sql_where_conditions:
            final_sql += " WHERE " + " AND ".join(sql_where_conditions)
        final_sql += sql_order

        self.logger.debug(
            f"実行SQL (get_yenjoy_races_to_update_for_step5): {final_sql}, Params: {params}"
        )

        results: List[Dict[str, Any]] = []
        try:
            fetched_results = self.execute_query(
                final_sql,
                tuple(params),
            )
            if fetched_results:
                self.logger.info(
                    f"{len(fetched_results)} 件のStep5更新対象レースが見つかりました。"
                )
                for row in fetched_results:
                    if row is None:
                        continue
                    race_date_val = row.get("race_date_db")
                    if isinstance(
                        race_date_val,
                        datetime,
                    ):
                        row["race_date_db"] = race_date_val.strftime("%Y-%m-%d")
                    elif isinstance(
                        race_date_val,
                        date,
                    ):
                        row["race_date_db"] = race_date_val.isoformat()
                    elif race_date_val is not None:
                        try:
                            if isinstance(
                                race_date_val,
                                str,
                            ):
                                dt_obj = datetime.strptime(
                                    str(race_date_val).split(" ")[0],
                                    "%Y-%m-%d",
                                )
                                row["race_date_db"] = dt_obj.strftime("%Y-%m-%d")
                            else:
                                row["race_date_db"] = str(race_date_val)
                                log_msg = (
                                    f"race_date_db が予期せぬ型 ({type(race_date_val)}) "
                                    f"ですが文字列に変換しました: {race_date_val}"
                                )
                                self.logger.warning(log_msg)
                        except (
                            ValueError,
                            AttributeError,
                        ) as e_conv:
                            log_msg = (
                                f"race_date_db の日付型変換に失敗: {race_date_val} "
                                f"type: {type(race_date_val)}, error: {e_conv}"
                            )
                            self.logger.warning(log_msg)
                results = fetched_results
            else:
                self.logger.info("Step5更新対象レースは見つかりませんでした。")
            return results
        except mysql.connector.Error as e_db:
            self.logger.error(
                f"Step5更新対象レースの取得中にDBエラー: {e_db}",
                exc_info=True,
            )
            return []
        except Exception as e_general:
            self.logger.error(
                f"Step5更新対象レースの取得中に予期せぬエラー: {e_general}",
                exc_info=True,
            )
            return []

    def execute_query_for_update(
        self,
        query: str,
        params: Union[tuple, Dict[str, Any]] = None,
        fetch_one: bool = False,
        dictionary: bool = True,
        conn: Optional[
            mysql.connector.connection.MySQLConnection
        ] = None,  # トランザクション内の接続を期待
        cursor: Optional[
            mysql.connector.cursor.MySQLCursor
        ] = None,  # トランザクション内のカーソルを期待
    ) -> Any:
        """
        SELECT ... FOR UPDATE クエリを実行し、結果を返す。
        必ず既存のトランザクション内で、提供された接続とカーソルを使用する。
        """
        if not conn or not cursor:
            self.logger.error(
                "execute_query_for_updateは既存の接続とカーソルが必須です。"
            )
            raise ValueError(
                "Connection and cursor must be provided for FOR UPDATE queries."
            )

        result = None

        # _execute_with_retry を使うべきだが、ここでは直接実行の例として記載
        # 実際には execute_query のように _execute_with_retry でラップする
        def _execute_for_update_internal():
            nonlocal result
            self.logger.debug(f"実行クエリ (FOR UPDATE): {query}, パラメータ: {params}")
            cursor.execute(query, params)
            if fetch_one:
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
            self.logger.debug(f"クエリ結果 (FOR UPDATE): {result}")

        try:
            # リトライ機構を適用して実行
            # 注意: _execute_with_retry は引数なしの関数を期待するので、
            # conn, cursor を渡す場合は、transaction_func のように、
            # _execute_with_retry の外側で conn, cursor を取得・管理し、
            # _execute_with_retryに渡す関数がそれらを利用するようにする。
            # 今回は既存のconn, cursorを直接使うため、_execute_with_retryの直接適用は難しい。
            # 代わりに、このメソッド自体がリトライロジックを持つか、
            # 呼び出し側がリトライを考慮する必要がある。
            # ここでは簡潔さのためリトライなしで記述。
            # 必要であれば、_execute_with_retryに渡せるようにラムダや部分関数でラップする。

            # self._execute_with_retry(lambda: _execute_for_update_internal()) # このようにラップできる

            # _execute_with_retryの構造に合わせるなら、
            # execute_query同様に、_execute_with_retryに渡す _execute 関数を定義する
            def _execute_op():
                _execute_for_update_internal()

            self._execute_with_retry(_execute_op)
            return result

        except mysql.connector.Error as e:
            self.logger.error(
                f"SELECT FOR UPDATEクエリ実行エラー ({query[:100]}...): {e}",
                exc_info=True,
            )
            # このメソッドはトランザクションの一部として呼び出されるため、
            # エラー発生時は例外を再送出して、呼び出し元のトランザクション管理に委ねる。
            # ここでconn.rollback()やconn.close()は行わない。
            raise
        # finally ブロックも不要 (接続とカーソルの管理は呼び出し元)
