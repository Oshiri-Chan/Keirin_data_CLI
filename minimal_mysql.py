#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最小限のMySQL接続クラス
アプリケーション内での接続問題を回避するための単純な実装
"""

import mysql.connector
import logging
from typing import Any, List, Dict, Optional
import sys

try:
    from database.db_accessor import KeirinDataAccessor  # 型整合のため
except Exception:  # pragma: no cover
    KeirinDataAccessor = object


class MinimalMySQLAccessor:
    """最小限のMySQL接続クラス - 確実に動作する実装"""

    def __init__(self, logger=None, config: Optional[Dict[str, Any]] = None):
        self.logger = logger or logging.getLogger(__name__)

        # デフォルト設定（必要に応じて上書き可能）
        default_conf: Dict[str, Any] = {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "password": "asasa2525",
            "database": "keirin_data_db",
            "charset": "utf8mb4",
            "autocommit": True,
            "connection_timeout": 3,
            "raise_on_warnings": False,
        }
        # WindowsではC拡張実装でのハング報告があるため純Python実装を強制
        try:
            if sys.platform.startswith("win"):
                default_conf["use_pure"] = True
        except Exception:
            pass
        if config:
            merged = default_conf.copy()
            merged.update(config)
            self.config = merged
        else:
            self.config = default_conf

        self.logger.info("MinimalMySQLAccessor初期化完了")

    def execute_query(
        self,
        query: str,
        params=None,
        fetch_one: bool = False,
        dictionary: bool = True,
        existing_conn=None,
        existing_cursor=None,
    ) -> Any:
        """SQLクエリを実行（既存接続/カーソルの受け取りに対応）"""
        conn = existing_conn
        cursor = existing_cursor
        created_conn = False
        try:
            if conn is None or cursor is None:
                conn = mysql.connector.connect(**self.config)
                cursor = conn.cursor(dictionary=dictionary)
                created_conn = True

            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)

            results = None
            if cursor.with_rows:
                if fetch_one:
                    results = cursor.fetchone()
                else:
                    results = cursor.fetchall()

            return results

        except Exception as e:
            self.logger.error(f"クエリ実行エラー: {e}")
            raise
        finally:
            if created_conn:
                try:
                    cursor.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

    def execute_many(
        self,
        query: str,
        params_list: List,
        existing_conn=None,
        existing_cursor=None,
    ) -> int:
        """バッチクエリ実行（既存接続/カーソルの受け取りに対応）"""
        if not params_list:
            return 0

        conn = existing_conn
        cursor = existing_cursor
        created_conn = False
        try:
            if conn is None or cursor is None:
                conn = mysql.connector.connect(**self.config)
                try:
                    conn.autocommit = True
                except Exception:
                    pass
                cursor = conn.cursor()
                created_conn = True

            # 仕様復帰: executemany を使用。ただし安全のためチャンク分割＋フォールバック
            chunk_size = 100
            total_affected = 0
            total = len(params_list)
            for start in range(0, total, chunk_size):
                end = min(start + chunk_size, total)
                chunk = params_list[start:end]
                try:
                    cursor.executemany(query, chunk)
                    try:
                        total_affected += cursor.rowcount or 0
                    except Exception:
                        pass
                    self.logger.debug(f"executemany chunk {start+1}-{end}/{total} 完了")
                except Exception as e_chunk:
                    # フォールバック: 1件ずつ実行
                    self.logger.warning(
                        f"executemany チャンク失敗のためフォールバック実行: {e_chunk}"
                    )
                    for idx, params in enumerate(chunk, start=start):
                        cursor.execute(query, params)
                        try:
                            total_affected += cursor.rowcount or 0
                        except Exception:
                            pass

            # autocommit=False の場合のみ commit
            try:
                if not getattr(conn, "autocommit", True):
                    conn.commit()
            except Exception:
                pass

            self.logger.info(
                f"バッチ実行完了: {total_affected}行処理 (chunked executemany with fallback)"
            )
            return total_affected

        except Exception as e:
            self.logger.error(f"バッチ実行エラー: {e}")
            if created_conn and conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if created_conn:
                try:
                    cursor.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

    def execute_scalar(self, query: str, params=None) -> Any:
        """単一値を返すクエリ（最小限実装）"""
        result = self.execute_query(query, params, fetch_one=True, dictionary=False)
        if result:
            return result[0]
        return None

    def test_connection(self) -> bool:
        """接続テスト"""
        try:
            result = self.execute_query("SELECT 1", fetch_one=True)
            return result is not None
        except Exception:
            return False

    def execute_in_transaction(self, func):
        """関数に (conn, cursor) を渡してトランザクション実行するラッパー"""
        conn = None
        cursor = None
        try:
            # トランザクション用に autocommit を無効化
            conn = mysql.connector.connect(**self.config)
            try:
                conn.autocommit = False
            except Exception:
                pass
            cursor = conn.cursor(dictionary=True)
            result = func(conn, cursor)
            conn.commit()
            return result
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            self.logger.error(f"トランザクション実行エラー: {e}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


class MinimalKeirinDataAdapter(KeirinDataAccessor):
    """KeirinDataAccessor として振る舞う極薄アダプター。

    内部で MinimalMySQLAccessor を用い、services 側の isinstance チェックを回避する。
    """

    def __init__(self, logger=None):
        # KeirinDataAccessor の重い初期化はスキップし、必要最低限のみ
        self.logger = logger or logging.getLogger(__name__)
        # config/config.ini の [MySQL] を読み取って反映
        conf = None
        try:
            from utils.config_manager import get_config_manager

            cfg = get_config_manager()
            host = cfg.get_value("MySQL", "host", fallback="127.0.0.1")
            user = cfg.get_value("MySQL", "user", fallback="root")
            password = cfg.get_value("MySQL", "password", fallback="")
            database = cfg.get_value("MySQL", "database", fallback="keirin_data_db")
            port = cfg.get_int("MySQL", "port", fallback=3306) or 3306
            conf = {
                "host": host,
                "user": user,
                "password": password,
                "database": database,
                "port": port,
            }
        except Exception:
            conf = None
        self._inner = MinimalMySQLAccessor(logger=self.logger, config=conf)

        # KeirinDataAccessor の一部属性をローカルに定義して互換性を確保
        # リトライ関連（execute_query_for_update -> _execute_with_retry で参照）
        self.DEADLOCK_ERROR_CODES = (1213, 1205)
        self.MAX_RETRY_ATTEMPTS = 3
        self.RETRY_DELAY_BASE = 0.5
        # ロック順序（未定義でも参照されるため空リストで用意）
        self.lock_order = []

    def execute_query(
        self,
        query: str,
        params=None,
        fetch_one: bool = False,
        dictionary: bool = True,
        existing_conn=None,
        existing_cursor=None,
    ) -> Any:
        return self._inner.execute_query(
            query,
            params=params,
            fetch_one=fetch_one,
            dictionary=dictionary,
            existing_conn=existing_conn,
            existing_cursor=existing_cursor,
        )

    def execute_many(
        self,
        query: str,
        params_list: List,
        existing_conn=None,
        existing_cursor=None,
    ) -> int:
        return self._inner.execute_many(
            query,
            params_list,
            existing_conn=existing_conn,
            existing_cursor=existing_cursor,
        )

    def execute_in_transaction(self, func):
        return self._inner.execute_in_transaction(func)

    def execute_scalar(self, query: str, params=None) -> Any:
        return self._inner.execute_scalar(query, params)

    def test_connection(self) -> bool:
        return self._inner.test_connection()
