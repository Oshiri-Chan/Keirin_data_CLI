#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
直接MySQL接続テスト
"""

import mysql.connector
import time


def test_direct_connection():
    print("MySQL直接接続テスト開始...")

    # 接続設定
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

    print(f"接続設定: {config}")

    try:
        print("接続を試行中...")
        start_time = time.time()

        conn = mysql.connector.connect(**config)

        elapsed = time.time() - start_time
        print(f"接続成功！ ({elapsed:.2f}秒)")

        # 簡単なクエリ
        cursor = conn.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"クエリ結果: {result}")

        # データベース一覧
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"データベース一覧: {databases}")

        cursor.close()
        conn.close()
        print("接続正常終了")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"接続失敗 ({elapsed:.2f}秒): {e}")
        return False


if __name__ == "__main__":
    success = test_direct_connection()
    print(f"結果: {'成功' if success else '失敗'}")
