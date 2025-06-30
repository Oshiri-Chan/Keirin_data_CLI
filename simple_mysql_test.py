#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
シンプルなMySQL接続テスト
"""

import mysql.connector
from utils.config_manager import get_config_manager


def main():
    print("🔍 シンプルMySQL接続テスト開始...")

    try:
        config = get_config_manager()

        # 設定値取得
        host = config.get_value("MySQL", "host")
        port = config.get_int("MySQL", "port")
        user = config.get_value("MySQL", "user")
        password = config.get_value("MySQL", "password")
        database = config.get_value("MySQL", "database")

        print(f"📋 接続先: {host}:{port}/{database} (user: {user})")

        # 直接接続テスト
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            collation="utf8mb4_unicode_ci",
        )

        print("✅ MySQL接続成功")

        # 基本クエリテスト
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT VERSION() as version")
        result = cursor.fetchone()
        print(f"📊 MySQLバージョン: {result['version']}")

        # テーブル数確認
        cursor.execute(
            "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = %s",
            (database,),
        )
        result = cursor.fetchone()
        print(f"📋 テーブル数: {result['count']}")

        cursor.close()
        connection.close()

        print("🎉 シンプル接続テスト完了!")
        return 0

    except Exception as e:
        print(f"❌ 接続エラー: {e}")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
