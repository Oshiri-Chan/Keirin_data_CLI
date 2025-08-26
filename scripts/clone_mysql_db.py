#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MySQL データベース複製スクリプト

config/config.ini の [MySQL] を既定値として使用。

使い方例:
  python scripts/clone_mysql_db.py --source-db keirin_data_db --target-db keirin_data_db_test \
      --mysql-bin "C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin"

引数を省略した場合は config.ini の host/user/password/port/database を使用し、
target-db は keirin_data_db_test になります。
"""

import argparse
import configparser
import os
import subprocess
import sys


def read_mysql_config(config_path: str):
    parser = configparser.ConfigParser()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"設定ファイルが見つかりません: {config_path}")
    parser.read(config_path, encoding="utf-8")
    if "MySQL" not in parser:
        raise ValueError("config.ini に [MySQL] セクションがありません")
    section = parser["MySQL"]
    return {
        "host": section.get("host", "127.0.0.1"),
        "port": section.getint("port", 3306),
        "user": section.get("user", "root"),
        "password": section.get("password", ""),
        "database": section.get("database", "keirin_data_db"),
    }


def run_mysql_command(
    mysql_bin: str, host: str, port: int, user: str, password: str, sql: str
):
    mysql_exe = os.path.join(mysql_bin, "mysql.exe")
    cmd = [
        mysql_exe,
        "--protocol=TCP",
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user,
        f"-p{password}",
        "--default-character-set=utf8mb4",
        "-e",
        sql,
    ]
    subprocess.run(cmd, check=True)


def dump_database(
    mysql_bin: str,
    host: str,
    port: int,
    user: str,
    password: str,
    source_db: str,
    dump_path: str,
):
    mysqldump_exe = os.path.join(mysql_bin, "mysqldump.exe")
    cmd = [
        mysqldump_exe,
        "--protocol=TCP",
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user,
        f"-p{password}",
        "--single-transaction",
        "--quick",
        "--routines",
        "--triggers",
        "--events",
        "--set-gtid-purged=OFF",
        "--default-character-set=utf8mb4",
        source_db,
    ]
    with open(dump_path, "wb") as f:
        subprocess.run(cmd, check=True, stdout=f)


def restore_database(
    mysql_bin: str,
    host: str,
    port: int,
    user: str,
    password: str,
    target_db: str,
    dump_path: str,
):
    mysql_exe = os.path.join(mysql_bin, "mysql.exe")
    cmd = [
        mysql_exe,
        "--protocol=TCP",
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user,
        f"-p{password}",
        "--default-character-set=utf8mb4",
        "--binary-mode",
        target_db,
    ]
    with open(dump_path, "rb") as f:
        subprocess.run(cmd, check=True, stdin=f)


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(project_root, "config", "config.ini")
    defaults = read_mysql_config(config_path)

    parser = argparse.ArgumentParser(
        description="Clone MySQL database using mysqldump/mysql"
    )
    parser.add_argument("--source-db", default=defaults["database"], help="ソースDB名")
    parser.add_argument(
        "--target-db", default=f"{defaults['database']}_test", help="ターゲットDB名"
    )
    parser.add_argument("--host", default=defaults["host"], help="ホスト")
    parser.add_argument("--port", type=int, default=defaults["port"], help="ポート")
    parser.add_argument("--user", default=defaults["user"], help="ユーザー")
    parser.add_argument("--password", default=defaults["password"], help="パスワード")
    parser.add_argument(
        "--mysql-bin",
        default=r"C:\\Program Files\\MySQL\\MySQL Server 8.0\\bin",
        help="mysql.exe/mysqldump.exe のあるディレクトリ",
    )
    parser.add_argument(
        "--dump-path",
        default=os.path.join(project_root, "dump_keirin.sql"),
        help="ダンプファイル出力パス",
    )

    args = parser.parse_args()

    try:
        # 1) ターゲットDB作成
        sql = (
            f"CREATE DATABASE IF NOT EXISTS {args.target_db} "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
        run_mysql_command(
            args.mysql_bin, args.host, args.port, args.user, args.password, sql
        )

        # 2) ソースDBをダンプ
        dump_database(
            args.mysql_bin,
            args.host,
            args.port,
            args.user,
            args.password,
            args.source_db,
            args.dump_path,
        )

        # 3) ダンプをターゲットDBにリストア
        restore_database(
            args.mysql_bin,
            args.host,
            args.port,
            args.user,
            args.password,
            args.target_db,
            args.dump_path,
        )

        print(
            "✓ データベース複製が完了しました:", f"{args.source_db} -> {args.target_db}"
        )
        print("ダンプファイル:", args.dump_path)
    except subprocess.CalledProcessError as e:
        print("✗ コマンド実行エラー:", e)
        sys.exit(1)
    except Exception as e:
        print("✗ 予期せぬエラー:", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
