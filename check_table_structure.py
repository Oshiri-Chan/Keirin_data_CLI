#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
regionsテーブル構造確認
"""

import mysql.connector
import sys
import traceback


def check_table_structure():
    """regionsテーブルの構造を確認"""
    try:
        print("=== regionsテーブル構造確認 ===")

        # 設定値（config.iniと同じ）
        config = {
            "host": "127.0.0.1",
            "user": "root",
            "password": "asasa2525",
            "database": "keirin_data_db",
            "port": 3306,
            "charset": "utf8mb4",
            "collation": "utf8mb4_unicode_ci",
            "autocommit": True,
        }

        # 接続試行
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # regionsテーブルの構造を確認
        print("\nregionsテーブルの構造:")
        cursor.execute("DESCRIBE regions")
        columns = cursor.fetchall()

        for column in columns:
            field, data_type, null, key, default, extra = column
            print(
                f"  {field}: {data_type} | NULL={null} | KEY={key} | DEFAULT={default}"
            )

        # 現在のregionsテーブルのデータを確認
        print("\n現在のregionsテーブルのデータ（先頭5件）:")
        cursor.execute("SELECT region_id, region_name FROM regions LIMIT 5")
        rows = cursor.fetchall()

        for row in rows:
            region_id, region_name = row
            print(f"  {region_id} ({len(region_id)}文字): {region_name}")

        # 接続クローズ
        cursor.close()
        connection.close()

        print("\n=== 構造確認完了 ===")
        return True

    except Exception as e:
        print(f"\n✗ 確認中にエラーが発生しました: {e}")
        print("\nスタックトレース:")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = check_table_structure()
    sys.exit(0 if success else 1)
