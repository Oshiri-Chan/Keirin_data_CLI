#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
シンプルなKeirinDataAccessor代替実装
"""

import mysql.connector
import logging
from typing import Any, Dict, List, Union


class SimpleKeirinDataAccessor:
    """シンプルなMySQL接続クラス"""

    def __init__(self, config_path="config/config.ini", logger=None):
        self.logger = logger or logging.getLogger(__name__)

        # 直接設定を指定（config.iniに依存しない）
        self.mysql_config = {
            "host": "127.0.0.1",
            "port": 3306,
            "user": "root",
            "password": "asasa2525",
            "database": "keirin_data_db",
            "charset": "utf8mb4",
            "collation": "utf8mb4_unicode_ci",
            "autocommit": True,
            "connection_timeout": 10,  # 10秒でタイムアウト
        }

        self.logger.info("SimpleKeirinDataAccessor初期化完了")

    def execute_query(
        self,
        query: str,
        params=None,
        fetch_one: bool = False,
        dictionary: bool = True,
        existing_conn=None,
        existing_cursor=None,
    ):
        """SQLクエリを実行"""
        conn = None
        cursor = None
        try:
            self.logger.info(f"クエリ実行開始: {query[:50]}...")
            self.logger.info("MySQL接続を作成中...")

            # 手動テストと完全に同じ設定を使用
            config = {
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

            self.logger.info(f"接続設定: {config}")
            conn = mysql.connector.connect(**config)
            self.logger.info("MySQL接続成功")

            self.logger.info("カーソル作成中...")
            cursor = conn.cursor(dictionary=dictionary)
            self.logger.info("カーソル作成成功")

            self.logger.info("クエリ実行中...")
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            self.logger.info("クエリ実行完了")

            results = None
            if cursor.with_rows:
                self.logger.info("結果取得中...")
                if fetch_one:
                    results = cursor.fetchone()
                else:
                    results = cursor.fetchall()
                self.logger.info(f"結果取得完了: {len(results) if results else 0}件")

            return results

        except Exception as e:
            self.logger.error(f"クエリエラー: {e}")
            raise
        finally:
            self.logger.info("リソースクリーンアップ中...")
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            self.logger.info("リソースクリーンアップ完了")

    def execute_many(
        self, query: str, params_list, existing_conn=None, existing_cursor=None
    ):
        """複数のSQLクエリを一括実行"""
        if not params_list:
            return 0

        conn = None
        cursor = None
        try:
            self.logger.info("MySQL接続を作成中...")
            conn = mysql.connector.connect(**self.mysql_config)
            self.logger.info("MySQL接続成功")

            self.logger.info("カーソル作成中...")
            cursor = conn.cursor()
            self.logger.info("カーソル作成成功")

            self.logger.info(f"executemany実行中... クエリ: {query[:100]}...")
            self.logger.info(f"パラメータ数: {len(params_list)}")
            cursor.executemany(query, params_list)
            self.logger.info("executemany完了")

            affected_rows = cursor.rowcount
            self.logger.info(f"execute_many成功: {affected_rows}行影響")
            return affected_rows

        except Exception as e:
            self.logger.error(f"execute_manyエラー: {e}")
            self.logger.error(f"失敗したクエリ: {query}")
            self.logger.error(
                f"パラメータ例: {params_list[:2] if params_list else 'なし'}"
            )
            raise
        finally:
            self.logger.info("リソースクリーンアップ中...")
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            self.logger.info("リソースクリーンアップ完了")

    def execute_scalar(self, query: str, params=None):
        """単一の値を返すクエリを実行"""
        result = self.execute_query(query, params, fetch_one=True, dictionary=False)
        if result:
            return result[0]
        return None
