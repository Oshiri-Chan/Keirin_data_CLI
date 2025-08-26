#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
シンプルなMySQL接続テスト
"""

import mysql.connector
import sys
import traceback


def test_mysql_simple():
    """シンプルなMySQL接続テスト"""
    try:
        print("=== MySQL接続テスト開始 ===")

        # 設定値（config.iniと同じ）
        config = {
            "host": "127.0.0.1",
            "user": "root",
            "password": "asasa2525",
            "database": "keirin_data_db",
            "port": 3306,
            "connection_timeout": 10,
            "autocommit": True,
        }

        print(f"接続先: {config['host']}:{config['port']}")
        print(f"データベース: {config['database']}")
        print(f"ユーザー: {config['user']}")

        # 接続試行
        print("\n接続を試行中...")
        connection = mysql.connector.connect(**config)

        print("✓ 接続成功！")

        # カーソル作成
        cursor = connection.cursor(dictionary=True)

        # 簡単なクエリ実行
        print("\nテストクエリを実行中...")
        cursor.execute("SELECT 1 as test_value, NOW() as `current_time`")
        result = cursor.fetchone()

        print(f"✓ クエリ実行成功: {result}")

        # データベース情報の確認
        cursor.execute("SELECT DATABASE() as current_db")
        db_result = cursor.fetchone()
        print(f"✓ 現在のデータベース: {db_result}")

        # テーブル一覧の取得
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"✓ テーブル数: {len(tables)}")
        if tables:
            print("  テーブル一覧:")
            for table in tables[:5]:  # 最初の5つだけ表示
                table_name = (
                    list(table.values())[0] if isinstance(table, dict) else str(table)
                )
                print(f"    - {table_name}")
            if len(tables) > 5:
                print(f"    ... and {len(tables) - 5} more")

        # 接続クローズ
        cursor.close()
        connection.close()

        print("\n=== MySQL接続テスト完了 ===")
        return True

    except mysql.connector.Error as err:
        print(f"\n✗ MySQL接続エラー: {err}")
        print(f"エラーコード: {err.errno}")
        print(f"SQLステート: {err.sqlstate}")
        return False

    except Exception as e:
        print(f"\n✗ 予期しないエラー: {e}")
        print("\nスタックトレース:")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_mysql_simple()
    sys.exit(0 if success else 1)
