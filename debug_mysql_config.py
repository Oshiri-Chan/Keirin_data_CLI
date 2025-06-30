#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
KeirinDataAccessorのMySQL設定デバッグスクリプト
"""

import logging
from database.db_accessor import KeirinDataAccessor


def main():
    print("🔍 KeirinDataAccessor MySQL設定デバッグ開始...")

    try:
        # ロガー設定
        logger = logging.getLogger("debug")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        logger.addHandler(handler)

        # KeirinDataAccessor初期化
        print("📋 KeirinDataAccessor初期化中...")
        db_accessor = KeirinDataAccessor("config/config.ini", logger)

        # mysql_config内容の確認
        print("\n📊 mysql_config内容:")
        for key, value in db_accessor.mysql_config.items():
            # パスワードは隠す
            if key.lower() == "password":
                print(f"  {key}: {'*' * len(str(value))}")
            else:
                print(f"  {key}: {value} (type: {type(value).__name__})")

        # 展開テスト
        print("\n🔧 config_for_direct展開テスト:")
        config_for_direct = {
            **db_accessor.mysql_config,
            "connection_timeout": 10,
            "autocommit": True,
        }

        for key, value in config_for_direct.items():
            if key.lower() == "password":
                print(f"  {key}: {'*' * len(str(value))}")
            else:
                print(f"  {key}: {value} (type: {type(value).__name__})")

        # 接続テスト
        print("\n🔗 接続テスト:")
        conn = db_accessor._get_new_connection()
        if conn and conn.is_connected():
            print(f"  ✅ 接続成功 (接続ID: {conn.connection_id})")

            # 簡単なクエリテスト
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT VERSION() as version")
            result = cursor.fetchone()
            print(f"  📊 MySQLバージョン: {result['version']}")

            cursor.close()
            conn.close()
            print("  🔐 接続をクローズしました")
        else:
            print("  ❌ 接続失敗")

        print("\n🎉 デバッグ完了!")
        return 0

    except Exception as e:
        print(f"❌ デバッグエラー: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
