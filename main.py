#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
競輪データ取得・管理システム用CLIツール
Keirin Data CLI - 競輪データの取得、更新、管理を行うコマンドラインツール
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional
import threading  # noqa: F401 (ハング診断用のフォールバックで使用する可能性)
import subprocess  # noqa: F401 (診断に使用)
import socket

# 自作モジュールのインポート
from api.winticket_api import WinticketAPI
from api.yenjoy_api import YenjoyAPI
from services.update_service import UpdateService
from utils.config_manager import get_config_manager
from utils.logger import setup_application_logger
from minimal_mysql import MinimalKeirinDataAdapter


class KeirinDataCLI:
    """競輪データCLIツールのメインクラス"""

    def __init__(self):
        """初期化"""
        self.logger = setup_application_logger()
        self.config = get_config_manager()
        self._setup_services()

    def _setup_services(self):
        """サービスの初期化"""
        try:
            # APIクライアントの作成
            self.winticket_api = WinticketAPI(logger=self.logger)
            self.yenjoy_api = YenjoyAPI(logger=self.logger)

            # データベースアクセサーの作成
            db_accessor_logger = logging.getLogger("KeirinDataAccessor")
            # KeirinDataAccessor と互換の薄いアダプターを使用
            self.db_accessor = MinimalKeirinDataAdapter(logger=db_accessor_logger)
            self.logger.info("MinimalMySQLAccessorを使用しています")

            # 接続を事前に確認（タイムアウト付き）
            self.logger.info("データベース接続を確認しています...")
            ping_ok, ping_err = self._safe_db_ping(timeout_seconds=5)
            if ping_ok:
                self.logger.info("データベース接続が確保されました（テスト成功）")
            else:
                self.logger.warning(
                    f"データベース接続テストがタイムアウト/失敗しました: {ping_err}"
                )

            # 更新サービスの作成
            self.logger.info("UpdateService の初期化を開始します")
            self.update_service = UpdateService(
                winticket_api=self.winticket_api,
                yenjoy_api=self.yenjoy_api,
                db_accessor=self.db_accessor,
                logger=self.logger,
                config_manager=self.config,
                default_max_workers=self.config.get_int(
                    "PERFORMANCE", "max_workers", fallback=5
                )
                or 5,
                winticket_rate_limit_wait=self.config.get_float(
                    "PERFORMANCE", "rate_limit_winticket", fallback=0.1
                )
                or 0.1,
                yenjoy_rate_limit_wait_html=self.config.get_float(
                    "PERFORMANCE", "rate_limit_yenjoy_html", fallback=1.0
                )
                or 1.0,
                yenjoy_rate_limit_wait_api=self.config.get_float(
                    "PERFORMANCE", "rate_limit_yenjoy_api", fallback=1.0
                )
                or 1.0,
            )

            self.logger.info("サービスの初期化が完了しました")

        except Exception as e:
            self.logger.error(f"サービスの初期化中にエラーが発生しました: {e}")
            raise

    def run(self):
        """CLIツールのメイン実行関数"""
        parser = self._create_parser()
        args = parser.parse_args()

        if hasattr(args, "func"):
            try:
                args.func(args)
            except KeyboardInterrupt:
                self.logger.info("処理が中断されました")
                sys.exit(130)
            except Exception as e:
                self.logger.error(f"処理中にエラーが発生しました: {e}")
                sys.exit(1)
        else:
            parser.print_help()
            sys.exit(1)

    def _create_parser(self) -> argparse.ArgumentParser:
        """コマンドライン引数パーサーの作成"""
        parser = argparse.ArgumentParser(
            prog="keirin-data-cli",
            description="競輪データ取得・管理システム用CLIツール",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=(
                "\n使用例:\n"
                "  # 通常の更新（更新日から前後2日分をチェック・更新）\n"
                "  python main.py update --mode check-range --step1 --step2 --step3 --step4 --step5\n\n"
                "  # 期間指定更新\n"
                "  python main.py update --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step3 --step5\n\n"
                "  # セットアップ（2018年から現在までの全データ保存）\n"
                "  python main.py update --mode setup --all-steps --force-update\n\n"
                "  # システム状態確認\n"
                "  python main.py status\n\n"
                "  # 設定確認\n"
                "  python main.py config --show\n\n"
                "  詳細なドキュメントは docs/CLI_USAGE.md を参照してください。\n"
            ),
        )

        # サブコマンドの作成
        subparsers = parser.add_subparsers(
            title="利用可能なコマンド",
            description="以下のコマンドが利用できます",
            dest="command",
            help="実行するコマンドを選択してください",
        )

        # 各コマンドの追加
        self._add_update_parser(subparsers)
        self._add_status_parser(subparsers)
        self._add_config_parser(subparsers)
        self._add_export_parser(subparsers)
        self._add_deploy_parser(subparsers)

        return parser

    def _add_update_parser(self, subparsers):
        """updateコマンドのパーサーを追加"""
        update_parser = subparsers.add_parser(
            "update",
            help="データ更新処理を実行",
            description="競輪データの更新処理を実行します",
        )

        # 更新モード
        update_parser.add_argument(
            "--mode",
            type=str,
            choices=["check-range", "period", "single-day", "setup"],
            default="check-range",
            help="更新モード (check-range | period | single-day | setup)",
        )

        # 期間指定
        update_parser.add_argument(
            "--start-date", type=str, help="開始日（YYYY-MM-DD形式）"
        )

        update_parser.add_argument(
            "--end-date", type=str, help="終了日（YYYY-MM-DD形式）"
        )

        # 単日更新用
        update_parser.add_argument(
            "--date", type=str, help="単日更新の日付（YYYY-MM-DD形式）"
        )

        # ステップ番号指定（必須）
        update_parser.add_argument(
            "--step",
            type=int,
            choices=[1, 2, 3, 4, 5],
            required=True,
            help="実行するステップ番号 (1〜5) を指定",
        )

        # オプション
        # 強制更新をデフォルト有効にし、--no-force-update で無効化可能にする
        update_parser.add_argument(
            "--force-update",
            dest="force_update",
            action="store_true",
            default=True,
            help="強制更新モード（デフォルト: 有効）",
        )
        update_parser.add_argument(
            "--no-force-update",
            dest="force_update",
            action="store_false",
            help="強制更新を無効化",
        )

        update_parser.add_argument(
            "--venue-codes",
            type=str,
            nargs="+",
            help="処理対象の会場コード（複数指定可能）",
        )

        update_parser.add_argument("--max-workers", type=int, help="最大並列処理数")

        update_parser.add_argument(
            "--dry-run",
            action="store_true",
            help="ドライランモード（更新せずに処理内容のみ表示）",
        )

        update_parser.add_argument(
            "--debug", action="store_true", help="デバッグモード"
        )

        update_parser.set_defaults(func=self.cmd_update)

    def _add_status_parser(self, subparsers):
        """statusコマンドのパーサーを追加"""
        status_parser = subparsers.add_parser(
            "status",
            help="システム状態を確認",
            description="データベースの状態やシステムの状態を確認します",
        )

        status_parser.add_argument(
            "--database", action="store_true", help="データベース接続状態を確認"
        )

        status_parser.add_argument(
            "--tables", action="store_true", help="テーブル状態を確認"
        )

        status_parser.add_argument(
            "--recent",
            type=int,
            default=7,
            help="最近N日間のデータ状況を確認（デフォルト: 7日）",
        )

        status_parser.set_defaults(func=self.cmd_status)

    def _add_config_parser(self, subparsers):
        """configコマンドのパーサーを追加"""
        config_parser = subparsers.add_parser(
            "config", help="設定管理", description="システム設定の確認・変更を行います"
        )

        config_parser.add_argument(
            "--show", action="store_true", help="現在の設定を表示"
        )

        config_parser.add_argument(
            "--set",
            type=str,
            nargs=2,
            metavar=("KEY", "VALUE"),
            help="設定値を変更（例: --set section.key value）",
        )

        config_parser.add_argument(
            "--test-connection", action="store_true", help="データベース接続をテスト"
        )

        config_parser.set_defaults(func=self.cmd_config)

    def _add_export_parser(self, subparsers):
        """exportコマンドのパーサーを追加"""
        export_parser = subparsers.add_parser(
            "export",
            help="データエクスポート",
            description="データベースからデータをエクスポートします",
        )

        export_parser.add_argument(
            "--format",
            type=str,
            choices=["csv", "json", "sql"],
            default="csv",
            help="エクスポート形式（デフォルト: csv）",
        )

        export_parser.add_argument(
            "--table", type=str, help="エクスポートするテーブル名"
        )

        export_parser.add_argument("--output", type=str, help="出力ファイルパス")

        export_parser.add_argument(
            "--start-date", type=str, help="データ期間の開始日（YYYY-MM-DD形式）"
        )

        export_parser.add_argument(
            "--end-date", type=str, help="データ期間の終了日（YYYY-MM-DD形式）"
        )

        export_parser.set_defaults(func=self.cmd_export)

    def _add_deploy_parser(self, subparsers):
        """deployコマンドのパーサーを追加"""
        deploy_parser = subparsers.add_parser(
            "deploy",
            help="DuckDBへのデプロイ",
            description="MySQLデータベースからDuckDBにデータをデプロイします",
        )

        deploy_parser.add_argument(
            "--output", type=str, help="DuckDBファイルの出力パス"
        )

        deploy_parser.add_argument(
            "--tables",
            type=str,
            nargs="+",
            help="デプロイするテーブル名（複数指定可能）",
        )

        deploy_parser.set_defaults(func=self.cmd_deploy)

    def cmd_update(self, args):
        """updateコマンドの実行"""
        self.logger.info("データ更新処理を開始します")

        # デバッグモードの設定
        if args.debug:
            self.logger.setLevel(logging.DEBUG)
            self.logger.info("デバッグモードが有効になりました")

        # ドライランモードの確認
        if args.dry_run:
            self.logger.info(
                "ドライランモード: 実際の更新は行わず、処理内容のみを表示します"
            )

        # ステップの確認
        steps = self._get_selected_steps(args)
        if not steps:
            self.logger.error("実行するステップが選択されていません")
            return

        self.logger.info(f"実行するステップ: {', '.join(steps)}")

        # ステップ実行前にデータベース接続を再確認（タイムアウト付き）
        self.logger.info("ステップ実行前にデータベース接続を確認しています...")
        ping_ok, ping_err = self._safe_db_ping(timeout_seconds=5)
        if ping_ok:
            self.logger.info("データベース接続確認完了")
        else:
            self.logger.warning(f"データベース接続確認がタイムアウト/失敗: {ping_err}")

        # 並列処理数の設定
        if args.max_workers:
            self.logger.info(f"並列処理数を{args.max_workers}に設定します")

        # 会場コードの確認
        venue_codes = args.venue_codes
        if venue_codes:
            self.logger.info(f"処理対象の会場コード: {', '.join(venue_codes)}")

        # 更新処理の実行
        try:
            start_time = time.time()

            if args.mode == "check-range":
                success, results = self._update_check_range(
                    steps, args.force_update, venue_codes
                )
            elif args.mode == "period":
                # 日本時間（JST）基準で、未指定なら前日〜翌日を自動設定
                if not args.start_date or not args.end_date:
                    jst = timezone(timedelta(hours=9))
                    today_jst = datetime.now(jst).date()
                    auto_start = (today_jst - timedelta(days=1)).strftime("%Y-%m-%d")
                    auto_end = (today_jst + timedelta(days=1)).strftime("%Y-%m-%d")
                    self.logger.info(
                        f"--start-date / --end-date 未指定のため、日本時間で {auto_start} 〜 {auto_end} を自動設定します"
                    )
                    start_date = auto_start
                    end_date = auto_end
                else:
                    start_date = args.start_date
                    end_date = args.end_date

                success, results = self._update_period(
                    start_date,
                    end_date,
                    steps,
                    args.force_update,
                    venue_codes,
                )
            elif args.mode == "single-day":
                # 日本時間（JST）基準で、未指定なら当日を自動設定
                jst = timezone(timedelta(hours=9))
                if not getattr(args, "date", None):
                    target_date = datetime.now(jst).date().strftime("%Y-%m-%d")
                    self.logger.info(
                        f"--date 未指定のため、日本時間の当日 {target_date} を自動設定します"
                    )
                else:
                    target_date = args.date

                success, results = self._update_period(
                    target_date,
                    target_date,
                    steps,
                    args.force_update,
                    venue_codes,
                )
            elif args.mode == "setup":
                success, results = self._update_setup(steps, args.force_update)
            else:
                self.logger.error(f"不明な更新モード: {args.mode}")
                return

            elapsed_time = time.time() - start_time
            self.logger.info(f"処理時間: {elapsed_time:.2f}秒")

            if success:
                self.logger.info("データ更新処理が正常に完了しました")
                self._print_update_results(results)
            else:
                self.logger.error("データ更新処理中にエラーが発生しました")
                sys.exit(1)

        except KeyboardInterrupt:
            self.logger.info("処理が中断されました")
            sys.exit(130)
        except Exception as e:
            self.logger.error(f"更新処理中にエラーが発生しました: {e}")
            sys.exit(1)

    def cmd_status(self, args):
        """statusコマンドの実行"""
        self.logger.info("システム状態を確認します")

        try:
            # データベース接続確認
            if args.database or not any([args.tables]):
                self._check_database_status()

            # テーブル状態確認
            if args.tables or not any([args.database]):
                self._check_table_status()

            # 最近のデータ状況確認
            self._check_recent_data_status(args.recent)

        except Exception as e:
            self.logger.error(f"状態確認中にエラーが発生しました: {e}")
            sys.exit(1)

    def cmd_config(self, args):
        """configコマンドの実行"""
        try:
            if args.show:
                self._show_config()
            elif args.set:
                self._set_config(args.set[0], args.set[1])
            elif args.test_connection:
                self._test_database_connection()
            else:
                self.logger.info("設定コマンドのオプションを指定してください")

        except Exception as e:
            self.logger.error(f"設定処理中にエラーが発生しました: {e}")
            sys.exit(1)

    def cmd_export(self, args):
        """exportコマンドの実行"""
        self.logger.info("データエクスポートを開始します")

        try:
            if not args.table:
                self.logger.error(
                    "エクスポートするテーブル名を指定してください（--table）"
                )
                return

            # エクスポート処理の実行
            # ここで実際のエクスポート処理を実装
            self.logger.info(
                f"テーブル '{args.table}' を {args.format} 形式でエクスポートします"
            )

            # 実装プレースホルダー
            self.logger.warning("エクスポート機能は実装中です")
        except Exception as e:
            self.logger.error(f"エクスポート処理中にエラーが発生しました: {e}")
            sys.exit(1)

    def cmd_deploy(self, args):
        """deployコマンドの実行"""
        self.logger.info("DuckDBデプロイを開始します")

        try:
            # デプロイ処理の実行
            # ここで実際のデプロイ処理を実装
            self.logger.info("DuckDBへのデプロイを実行します")

            # 実装プレースホルダー
            self.logger.warning("デプロイ機能は実装中です")
        except Exception as e:
            self.logger.error(f"デプロイ処理中にエラーが発生しました: {e}")
            sys.exit(1)

    def _get_selected_steps(self, args) -> List[str]:
        """選択されたステップのリストを取得"""
        # 新仕様: --step で指定された単一ステップのみ実行
        if hasattr(args, "step") and args.step:
            return [f"step{args.step}"]

        return []

    def _update_check_range(
        self, steps: List[str], force_update: bool, venue_codes: Optional[List[str]]
    ) -> tuple:
        """check-rangeモードの更新処理"""
        self.logger.info("チェックレンジモードで更新を実行します")

        # 現在日から前後2日の範囲で更新
        today = datetime.now()
        start_date = (today - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = (today + timedelta(days=2)).strftime("%Y-%m-%d")

        return self.update_service.update_period_step_by_step(
            start_date_str=start_date,
            end_date_str=end_date,
            steps=steps,
            venue_codes=venue_codes,
            force_update_all=force_update,
        )

    def _update_period(
        self,
        start_date: str,
        end_date: str,
        steps: List[str],
        force_update: bool,
        venue_codes: Optional[List[str]],
    ) -> tuple:
        """periodモードの更新処理"""
        self.logger.info(
            f"期間指定モードで更新を実行します（{start_date} ～ {end_date}）"
        )

        return self.update_service.update_period_step_by_step(
            start_date_str=start_date,
            end_date_str=end_date,
            steps=steps,
            venue_codes=venue_codes,
            force_update_all=force_update,
        )

    def _update_setup(self, steps: List[str], force_update: bool) -> tuple:
        """setupモードの更新処理"""
        self.logger.info("セットアップモードで更新を実行します（2018年から現在まで）")

        # 2018年1月1日から現在までの期間で更新
        start_date = "2018-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")

        return self.update_service.update_period_step_by_step(
            start_date_str=start_date,
            end_date_str=end_date,
            steps=steps,
            force_update_all=force_update,
        )

    def _print_update_results(self, results: dict):
        """更新結果の表示"""
        if not results:
            return

        print("\n=== 更新結果 ===")
        for step, result in results.items():
            if isinstance(result, dict):
                status = "成功" if result.get("success", False) else "失敗"
                count = result.get("count", 0)
                print(f"{step}: {status} ({count}件)")
            else:
                print(f"{step}: {result}")

    def _check_database_status(self):
        """データベース接続状態の確認"""
        print("データベース接続状態を確認しています...")

        try:
            result = self.db_accessor.execute_query("SELECT 1 as test")
            if result:
                print("✓ データベース接続: 正常")
            else:
                print("✗ データベース接続: 異常")
        except Exception as e:
            print(f"✗ データベース接続: エラー ({e})")

    def _check_table_status(self):
        """テーブル状態の確認"""
        print("テーブル状態を確認しています...")

        try:
            tables_query = "SHOW TABLES"
            result = self.db_accessor.execute_query(tables_query)

            if result:
                print(f"✓ テーブル数: {len(result)}個")
                for row in result:
                    table_name = list(row.values())[0]
                    count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                    try:
                        count_result = self.db_accessor.execute_query(count_query)
                        count = count_result[0]["count"] if count_result else 0
                        print(f"  - {table_name}: {count:,}件")
                    except Exception:
                        print(f"  - {table_name}: レコード数取得エラー")
            else:
                print("✗ テーブルが見つかりません")
        except Exception as e:
            print(f"✗ テーブル状態確認エラー: {e}")

    def _check_recent_data_status(self, days: int):
        """最近のデータ状況確認"""
        print(f"\n最近{days}日間のデータ状況を確認しています...")

        try:
            # 最近のレースデータ数を確認
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            end_date = datetime.now().strftime("%Y-%m-%d")

            query = """
            SELECT DATE(start_at) as race_date, COUNT(*) as race_count
            FROM races
            WHERE DATE(start_at) BETWEEN %s AND %s
            GROUP BY DATE(start_at)
            ORDER BY race_date DESC
            """

            result = self.db_accessor.execute_query(query, (start_date, end_date))

            if result:
                print("日付別レース数:")
                for row in result:
                    print(f"  {row['race_date']}: {row['race_count']}レース")
            else:
                print("最近のレースデータが見つかりません")

        except Exception as e:
            print(f"最近のデータ状況確認エラー: {e}")

    def _show_config(self):
        """現在の設定を表示"""
        print("現在の設定:")

        # 設定ファイルの内容を表示
        if hasattr(self.config, "_config") and self.config._config:
            for section_name in self.config._config.sections():
                print(f"\n[{section_name}]")
                for key, value in self.config._config.items(section_name):
                    print(f"  {key} = {value}")
        else:
            print("設定ファイルが読み込まれていません")

    def _set_config(self, key: str, value: str):
        """設定値の変更"""
        print("設定を変更します: {} = {}".format(key, value))
        # 実装プレースホルダー
        self.logger.warning("設定変更機能は実装中です")

    def _test_database_connection(self):
        """データベース接続のテスト"""
        print("データベース接続をテストしています...")

        try:
            start_time = time.time()
            result = self.db_accessor.execute_query("SELECT VERSION() as version")
            elapsed_time = time.time() - start_time

            if result:
                version = result[0]["version"]
                print("✓ 接続成功")
                print(f"  データベースバージョン: {version}")
                print(f"  応答時間: {elapsed_time:.3f}秒")
            else:
                print("✗ 接続テスト失敗")
        except Exception as e:
            print(f"✗ 接続エラー: {e}")

    def _safe_db_ping(self, timeout_seconds: int = 5):
        """DB簡易疎通チェックをタイムアウト付きで実行

        戻り値: (success: bool, error_message: Optional[str])
        """
        # 1) ソケットレベルの疎通確認（すぐ返す）
        try:
            host = self.config.get_value("MySQL", "host", fallback="127.0.0.1")
            port = int(self.config.get_int("MySQL", "port", fallback=3306) or 3306)
            with socket.create_connection((host, port), timeout=3):
                pass
        except Exception as e:
            return False, f"socket connect failed: {e}"

        # 2) サブプロセスで実接続（ハングしてもタイムアウトで返す）
        try:
            python_snippet = (
                "from utils.config_manager import get_config_manager; "
                "from minimal_mysql import MinimalMySQLAccessor; "
                "cfg=get_config_manager(); "
                "conf={'host':cfg.get_value('MySQL','host','127.0.0.1'), "
                "      'user':cfg.get_value('MySQL','user','root'), "
                "      'password':cfg.get_value('MySQL','password',''), "
                "      'database':cfg.get_value('MySQL','database','keirin_data_db'), "
                "      'port':cfg.get_int('MySQL','port',3306)}; "
                "a=MinimalMySQLAccessor(config=conf); "
                "print('OK' if a.test_connection() else 'NG')"
            )
            cmd = [sys.executable, "-c", python_snippet]
            completed = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout_seconds
            )
            stdout = (completed.stdout or "").strip()
            stderr = (completed.stderr or "").strip()
            if completed.returncode == 0 and stdout.startswith("OK"):
                return True, None
            return (
                False,
                f"subprocess ret={completed.returncode}, out={stdout}, err={stderr}",
            )
        except subprocess.TimeoutExpired:
            return False, "subprocess timeout"
        except Exception as e:
            return False, f"subprocess error: {e}"


def main():
    """メイン関数"""
    try:
        cli = KeirinDataCLI()
        cli.run()
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(130)
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
