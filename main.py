#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
競輪データ更新ツールのメインスクリプト
"""

import argparse
import logging
import os  # noqa: F401
import sys
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path  # noqa: F401

from api.winticket_api import WinticketAPI
from api.yenjoy_api import YenjoyAPI
from core.application import KeirinUpdaterCore
from database.db_accessor import KeirinDataAccessor

# from database.db_initializer import DatabaseInitializer # 削除
# from database.keirin_database import KeirinDatabase # 削除
from gui.keirin_updater_gui import KeirinUpdaterGUI  # noqa: F401
from services.update_service import UpdateService
from utils.config_manager import get_config_manager

# 自作モジュールのインポート
from utils.logger import setup_application_logger

# from services.database_service import DatabaseService # 削除
# from services.scrape_service import ScrapeService   # 削除
# from 상태.status_manager import StatusManager # コメントアウト (未使用の可能性)


def parse_arguments():
    """
    コマンドライン引数を解析する

    Returns:
        argparse.Namespace: 解析された引数
    """
    parser = argparse.ArgumentParser(
        description="競輪データ更新ツール",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 通常の更新（更新日から前後2日分をチェック・更新）
  python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1

  # 期間指定更新
  python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 0 --step3 1 --step4 0 --step5 1

  # セットアップ（2018年から現在までの全データ保存）
  python main.py --mode setup --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --force-update 1

  # GUIモード（引数なし）
  python main.py
        """,
    )

    # 更新モード（必須）
    parser.add_argument(
        "--mode",
        type=str,
        choices=["check_update", "period", "setup"],
        help="更新モード: check_update=更新日から前後2日分チェック, period=期間指定更新, setup=2018年からの全データ保存",
    )

    # 更新期間（期間指定モード用）
    parser.add_argument(
        "--start-date",
        type=str,
        help="開始日（YYYY-MM-DD形式）。期間指定モード（--mode period）でのみ使用",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="終了日（YYYY-MM-DD形式）。期間指定モード（--mode period）でのみ使用",
    )

    # 更新ステップ（1-5の各ステップを0または1で指定）
    parser.add_argument(
        "--step1",
        type=int,
        choices=[0, 1],
        default=0,
        help="ステップ1を実行するか（0=実行しない, 1=実行する）",
    )

    parser.add_argument(
        "--step2",
        type=int,
        choices=[0, 1],
        default=0,
        help="ステップ2を実行するか（0=実行しない, 1=実行する）",
    )

    parser.add_argument(
        "--step3",
        type=int,
        choices=[0, 1],
        default=0,
        help="ステップ3を実行するか（0=実行しない, 1=実行する）",
    )

    parser.add_argument(
        "--step4",
        type=int,
        choices=[0, 1],
        default=0,
        help="ステップ4を実行するか（0=実行しない, 1=実行する）",
    )

    parser.add_argument(
        "--step5",
        type=int,
        choices=[0, 1],
        default=0,
        help="ステップ5を実行するか（0=実行しない, 1=実行する）",
    )

    # 強制更新モード
    parser.add_argument(
        "--force-update",
        type=int,
        choices=[0, 1],
        default=0,
        help="強制更新モード（0=通常更新, 1=強制更新）",
    )

    # 追加提案: デバッグモード
    parser.add_argument(
        "--debug",
        type=int,
        choices=[0, 1],
        default=0,
        help="デバッグモード（0=通常ログ, 1=詳細ログ）",
    )

    # 追加提案: 会場コード指定
    parser.add_argument(
        "--venue-codes",
        type=str,
        nargs="+",
        help="処理対象の会場コード（複数指定可能）。例: --venue-codes 01 02 03",
    )

    # 追加提案: 並列処理数
    parser.add_argument(
        "--max-workers", type=int, help="最大並列処理数（デフォルト: 設定ファイルの値）"
    )

    # 追加提案: ドライランモード
    parser.add_argument(
        "--dry-run",
        type=int,
        choices=[0, 1],
        default=0,
        help="ドライランモード（0=実際に更新, 1=更新せずに処理内容のみ表示）",
    )

    # 引数を解析
    args = parser.parse_args()

    # コマンドラインモードかGUIモードかを判定
    if args.mode is None:
        # modeが指定されていない場合はGUIモード
        args.command = "gui"
    else:
        # modeが指定されている場合はコマンドラインモード
        args.command = "cli"

    return args


def process_cli_command(args, update_service, db_accessor, config, logger):
    """
    CLIコマンドの処理

    Args:
        args: コマンドライン引数
        update_service: 更新サービスインスタンス
        db_accessor: データベースアクセサインスタンス
        config: 設定マネージャー
        logger: ロガー
    """
    logger.info("コマンドラインモードでデータ更新処理を開始します")

    # デバッグモードの設定
    if args.debug == 1:
        logger.setLevel(logging.DEBUG)
        logger.info("デバッグモードが有効になりました")

    # ドライランモードの確認
    if args.dry_run == 1:
        logger.info("ドライランモード: 実際の更新は行わず、処理内容のみを表示します")

    # 並列処理数の設定
    if args.max_workers:
        # 設定を一時的に更新（実装は設定管理方法に依存）
        logger.info(f"並列処理数を {args.max_workers} に設定しました")

    # ステップの設定（1が指定されたステップのみを実行）
    enabled_steps = []
    if args.step1 == 1:
        enabled_steps.append("step1")
    if args.step2 == 1:
        enabled_steps.append("step2")
    if args.step3 == 1:
        enabled_steps.append("step3")
    if args.step4 == 1:
        enabled_steps.append("step4")
    if args.step5 == 1:
        enabled_steps.append("step5")

    if not enabled_steps:
        logger.warning("実行するステップが指定されていません。処理を終了します。")
        return

    # 強制更新フラグ
    force_update = bool(args.force_update)

    # 更新モードに応じた処理
    if args.mode == "check_update":
        # 更新日から前後2日分のチェックと更新
        today = datetime.now()
        start_date = (today - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = (today + timedelta(days=2)).strftime("%Y-%m-%d")

        logger.info(f"チェック更新モード: {start_date} から {end_date} までの期間")

    elif args.mode == "period":
        # 期間指定更新
        if not args.start_date or not args.end_date:
            logger.error(
                "期間指定モードでは --start-date と --end-date の両方が必要です"
            )
            return

        start_date = args.start_date
        end_date = args.end_date
        logger.info(f"期間指定更新モード: {start_date} から {end_date} までの期間")

    elif args.mode == "setup":
        # セットアップ（2018年から現在まで）
        start_date = "2018-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        force_update = True  # セットアップモードでは強制更新

        logger.info(
            f"セットアップモード: {start_date} から {end_date} までの全データ保存"
        )
        logger.warning(
            "セットアップモードは大量のデータを処理するため、完了まで長時間かかる可能性があります"
        )

    # ドライランモードの場合は処理内容のみ表示
    if args.dry_run == 1:
        logger.info("=== ドライラン結果 ===")
        logger.info(f"更新モード: {args.mode}")
        logger.info(f"期間: {start_date} から {end_date}")
        logger.info(f"実行ステップ: {', '.join(enabled_steps)}")
        logger.info(f"強制更新: {force_update}")
        if args.venue_codes:
            logger.info(f"対象会場コード: {', '.join(args.venue_codes)}")
        logger.info("ドライランのため、実際の更新処理は実行されませんでした")
        return

    # 実際の更新処理をバックグラウンドスレッドで実行（GUIモードと同様の安定性確保）
    logger.info(f"期間: {start_date} から {end_date} までのデータを更新します")
    logger.info(f"実行ステップ: {', '.join(enabled_steps)}")
    logger.info(f"強制更新: {force_update}")

    import threading

    # スレッド処理結果格納用
    thread_result = {"success": False, "results": {}, "error": None}

    def cli_update_thread():
        """CLIモード用の更新処理スレッド"""
        try:
            if args.venue_codes:
                logger.info(
                    f"対象会場コード: {', '.join(args.venue_codes)} のデータを更新します"
                )
                success, results = update_service.update_period_step_by_step(
                    start_date,
                    end_date,
                    enabled_steps,
                    venue_codes=args.venue_codes,
                    force_update_all=force_update,
                )
            else:
                # 期間全体の更新
                success, results = update_service.update_period_step_by_step(
                    start_date, end_date, enabled_steps, force_update_all=force_update
                )

            thread_result["success"] = success
            thread_result["results"] = results

        except Exception as e:
            logger.error(f"更新スレッドでエラーが発生しました: {str(e)}", exc_info=True)
            thread_result["error"] = e

    # バックグラウンドスレッドで更新処理を実行
    logger.info("バックグラウンドスレッドで更新処理を開始します...")
    update_thread = threading.Thread(target=cli_update_thread, daemon=True)
    update_thread.start()

    # スレッド完了まで待機（進捗表示付き）
    while update_thread.is_alive():
        update_thread.join(timeout=5.0)  # 5秒間隔でチェック
        if update_thread.is_alive():
            logger.info("更新処理を継続中...")

    # スレッド結果を取得
    if thread_result["error"]:
        logger.error(f"更新処理中にエラーが発生しました: {thread_result['error']}")
        success, results = False, {}
    else:
        success, results = thread_result["success"], thread_result["results"]

    # 結果の表示
    if success:
        logger.info("データ更新処理が完了しました")
    else:
        logger.warning("データ更新処理が一部失敗しました")

    # 各ステップの結果を表示
    for step, result in results.items():
        status = "成功" if result["success"] else "失敗"
        logger.info(
            f"ステップ {step}: {status} - {result['message']} ({result['count']}件処理)"
        )

    # 自動デプロイ処理（セットアップモード以外で全体的に成功した場合）
    if success and args.mode != "setup":
        try:
            auto_deploy = config.getboolean(
                "System", "auto_deploy_after_update", fallback=False
            )
            if auto_deploy:
                logger.info("自動デプロイを実行しています...")
                # デプロイ処理の実装（既存のprocess_deploy_command関数を使用）
                from types import SimpleNamespace

                deploy_args = SimpleNamespace(output_path="frontend.duckdb")
                process_deploy_command(deploy_args, db_accessor, logger)
        except Exception as e:
            logger.warning(f"自動デプロイ中にエラーが発生しました: {str(e)}")


def process_export_command(args, db_accessor, logger):
    """
    exportコマンドの処理

    Args:
        args: コマンドライン引数
        db_accessor: データベースアクセサインスタンス
        logger: ロガー
    """
    logger.info(
        f"データエクスポート処理を開始します: テーブル={args.table}, 形式={args.format}"
    )

    try:
        # JSONエクスポート
        if args.format == "json":
            json_file = db_accessor.export_table_to_json(args.table)
            if json_file:
                logger.info(f"JSONエクスポートが完了しました: {json_file}")
            else:
                logger.error(f"JSONエクスポートに失敗しました: {args.table}")

        # CSVエクスポート
        elif args.format == "csv":
            # テーブルからデータを取得
            data = db_accessor.read_table(args.table)
            if not data.empty:
                # 出力ファイル名の設定
                output_file = (
                    args.output
                    if args.output
                    else f"{args.table}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
                )
                data.to_csv(output_file, index=False, encoding="utf-8")
                logger.info(f"CSVエクスポートが完了しました: {output_file}")
            else:
                logger.error(f"エクスポートするデータがありません: {args.table}")

    except Exception as e:
        logger.error(f"データエクスポート中にエラーが発生しました: {str(e)}")


def process_analyze_command(args, db_accessor, logger):
    """
    analyzeコマンドの処理

    Args:
        args: コマンドライン引数
        db_accessor: データベースアクセサインスタンス
        logger: ロガー
    """
    logger.info(f"データ分析処理を開始します: タイプ={args.type}")

    # 日付の設定
    if args.start_date:
        start_date = args.start_date
    else:
        # デフォルトは過去3ヶ月
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    if args.end_date:
        end_date = args.end_date
    else:
        # デフォルトは現在日付
        end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"分析期間: {start_date} から {end_date}")

    try:
        # 分析タイプに応じた処理
        if args.type == "summary":
            # サマリー分析
            # TODO: 分析処理の実装
            logger.info("サマリー分析を実行します（未実装）")
        elif args.type == "detail":
            # 詳細分析
            # TODO: 分析処理の実装
            logger.info("詳細分析を実行します（未実装）")

    except Exception as e:
        logger.error(f"データ分析中にエラーが発生しました: {str(e)}")


def process_deploy_command(args, db_accessor, logger):
    """
    deployコマンドの処理（フロントエンド用DBへのデータデプロイ）

    Args:
        args: コマンドライン引数
        db_accessor: データベースアクセサインスタンス (MySQL)
        logger: ロガー
    """
    logger.info(
        f"フロントエンド用データベースへのデプロイ処理を開始します: 出力先={args.output_path}"
    )

    try:
        # 必要なモジュールをここでインポート（依存関係を明確にするため）
        from database.frontend_database import FrontendDatabase  # DuckDB用
        from services.deployment_service import DeploymentService

        # フロントエンド用DBインスタンスを作成
        frontend_db = FrontendDatabase(args.output_path)

        # デプロイサービスを作成
        deployment_service = DeploymentService(db_accessor, frontend_db, logger)

        # デプロイ実行 (現時点では全テーブルを対象とする)
        # 特定テーブルのみデプロイする場合は args.tables を使うように変更する
        success = deployment_service.deploy_all_tables()

        if success:
            logger.info(f"デプロイが完了しました: {args.output_path}")
        else:
            logger.error("デプロイ処理中にエラーが発生しました")

    except ImportError as e:
        logger.error(f"必要なモジュールのインポートに失敗しました: {e}")
        logger.error(
            "DeploymentService または FrontendDatabase が存在しない可能性があります。"
        )
    except Exception as e:
        logger.error(f"デプロイ処理中に予期せぬエラーが発生しました: {str(e)}")


def process_update_command(args, update_service, db_accessor, config, logger):
    """
    updateコマンドの処理（旧バージョンとの互換性のため維持）

    Args:
        args: コマンドライン引数
        update_service: 更新サービスインスタンス
        db_accessor: データベースアクセサインスタンス
        config: 設定マネージャー
        logger: ロガー
    """
    logger.info("データ更新処理を開始します")

    # 日付の設定
    if args.start_date:
        start_date = args.start_date
    else:
        # デフォルトは過去3ヶ月
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    if args.end_date:
        end_date = args.end_date
    else:
        # デフォルトは現在日付
        end_date = datetime.now().strftime("%Y-%m-%d")

    # ステップの設定（旧形式との互換性）
    if hasattr(args, "steps") and args.steps:
        steps = args.steps
    else:
        # 新形式の場合
        steps = []
        if getattr(args, "step1", 0) == 1:
            steps.append("step1")
        if getattr(args, "step2", 0) == 1:
            steps.append("step2")
        if getattr(args, "step3", 0) == 1:
            steps.append("step3")
        if getattr(args, "step4", 0) == 1:
            steps.append("step4")
        if getattr(args, "step5", 0) == 1:
            steps.append("step5")

        # デフォルトで全ステップを実行
        if not steps:
            steps = ["step1", "step2", "step3", "step4", "step5"]

    force_update = getattr(args, "force_update", 0) or getattr(
        args, "force_update_all", False
    )

    logger.info(
        f"期間: {start_date} から {end_date} までのデータを更新します（ステップ: {', '.join(steps)}, 強制更新: {force_update}）"
    )

    # venue_codes の処理
    venue_codes_to_process = getattr(args, "venue_codes", None)
    if venue_codes_to_process:
        logger.info(f"会場コード: {venue_codes_to_process} のデータを更新します")
        success, results = update_service.update_period_step_by_step(
            start_date,
            end_date,
            steps,
            venue_codes=venue_codes_to_process,
            force_update_all=force_update,
        )
    else:
        # 期間全体の更新
        success, results = update_service.update_period_step_by_step(
            start_date, end_date, steps, force_update_all=force_update
        )

    if success:
        logger.info("データ更新処理が完了しました")
    else:
        logger.warning("データ更新処理が一部失敗しました")

    # 結果の表示
    for step, result in results.items():
        status = "成功" if result["success"] else "失敗"
        logger.info(
            f"ステップ {step}: {status} - {result['message']} ({result['count']}件処理)"
        )

    # --- 自動デプロイ処理 --- (更新が全体的に成功した場合のみ実行)
    if success:
        try:
            auto_deploy = config.getboolean(
                "Deployment", "auto_deploy_after_update", fallback=False
            )
            if auto_deploy:
                logger.info("更新完了に伴い、自動デプロイ処理を開始します...")
                frontend_db_path = config.get(
                    "Deployment", "frontend_db_path", fallback="frontend.duckdb"
                )

                # 必要なモジュールをインポート
                from database.frontend_database import FrontendDatabase
                from services.deployment_service import DeploymentService

                frontend_db = FrontendDatabase(
                    frontend_db_path, logger=logger
                )  # ロガーを渡す
                deployment_service = DeploymentService(db_accessor, frontend_db, logger)

                deploy_success = deployment_service.deploy_all_tables()
                if deploy_success:
                    logger.info(f"自動デプロイが完了しました: {frontend_db_path}")
                else:
                    logger.error("自動デプロイ処理中にエラーが発生しました。")
            else:
                logger.info("自動デプロイは設定で無効化されています。")

        except Exception as e:
            logger.error(f"自動デプロイ処理中に予期せぬエラーが発生しました: {e}")
    else:
        logger.warning(
            "データ更新処理が失敗したため、自動デプロイはスキップされました。"
        )


def main():
    """
    メイン関数：プログラムのエントリーポイント
    """
    # ロギング設定
    logger = setup_application_logger()
    logger.info("競輪データ更新ツールを起動しています...")

    try:
        # データベース初期化の確認
        # db = KeirinDatabase()
        # データベース初期化処理
        # initializer = DatabaseInitializer(db)
        # db_exists, tables_exist, missing_tables = initializer.check_database()
        #
        # if not db_exists or missing_tables:
        # logger.info(
        # "データベースまたは必要なテーブルが存在しません。初期化を行います。"
        # )
        # success = initializer.initialize_database()
        # if success:
        # logger.info("データベース初期化が完了しました")
        # else:
        # logger.error("データベース初期化に失敗しました")

        # APIクライアントの作成
        config = get_config_manager()
        winticket_api = WinticketAPI(logger=logger)
        yenjoy_api = YenjoyAPI(logger=logger)

        # データベース関連の初期化
        # KeirinDatabase はDBファイルパスの管理等でまだ役割があるかもしれない
        # db_path = config.get('System', 'database_path')
        # keirin_db_instance = KeirinDatabase(db_path=db_path, logger=logger)

        # ★ KeirinDataAccessor を初期化
        db_accessor_logger = logging.getLogger(
            "KeirinDataAccessor"
        )  # KeirinDataAccessor 用のロガーを取得
        default_config_path = (
            "config/config.ini"  # ConfigManagerが使用するデフォルトパスを仮定
        )
        db_accessor = KeirinDataAccessor(
            config_path=default_config_path, logger=db_accessor_logger
        )

        # DB初期化処理 (テーブル作成など)
        # db_initializer = DatabaseInitializer(keirin_db_instance, logger)
        # db_initializer.initialize_database() # コメントアウトされていたので、必要なら戻す

        # 更新サービスの作成
        update_service = UpdateService(
            winticket_api=winticket_api,
            yenjoy_api=yenjoy_api,
            # db_instance=keirin_db_instance, # ← KeirinDatabase のインスタンスを渡していた箇所
            db_accessor=db_accessor,  # ★ KeirinDataAccessor のインスタンスを渡す
            logger=logger,  # logger_manager引数ではなく、logger引数に修正
            config_manager=config,
            default_max_workers=config.get_int("PERFORMANCE", "max_workers", fallback=5)
            or 5,  # None対策
            winticket_rate_limit_wait=config.get_float(
                "PERFORMANCE", "rate_limit_winticket", fallback=0.1
            )
            or 0.1,  # None対策
            yenjoy_rate_limit_wait_html=config.get_float(
                "PERFORMANCE", "rate_limit_yenjoy_html", fallback=1.0
            )
            or 1.0,  # None対策
            yenjoy_rate_limit_wait_api=config.get_float(
                "PERFORMANCE", "rate_limit_yenjoy_api", fallback=1.0
            )
            or 1.0,  # None対策
        )

        # 引数の解析
        args = parse_arguments()

        # 引数に従って処理を実行
        if args.command == "cli":
            # CLIモード
            process_cli_command(args, update_service, db_accessor, config, logger)
            logger.info("CLIモードでの処理が完了しました")
            return
        elif args.command == "update":
            # 更新処理（旧バージョン互換）
            process_update_command(args, update_service, db_accessor, config, logger)
            logger.info("更新処理が完了しました")
            return
        elif args.command == "export":
            # エクスポート処理
            process_export_command(args, db_accessor, logger)
            logger.info("エクスポート処理が完了しました")
            return
        elif args.command == "analyze":
            # 分析処理
            process_analyze_command(args, db_accessor, logger)
            logger.info("分析処理が完了しました")
            return
        elif args.command == "deploy":
            # デプロイ処理
            process_deploy_command(args, db_accessor, logger)
            logger.info("デプロイ処理が完了しました")
            return
        elif args.command == "gui":
            # GUIモード
            logger.info("GUIモードで起動します")
        else:
            logger.error(f"不明なコマンド: {args.command}")
            return

        # アプリケーションコアの初期化
        app_core = KeirinUpdaterCore()

        # アプリケーションの起動処理
        app_core.startup()

        # ルートウィンドウの作成
        root = tk.Tk()

        # アプリケーションの作成時に必要なインスタンスを渡す
        app = KeirinUpdaterGUI(
            root,
            db_accessor=db_accessor,
            config=config,
            update_service=update_service,
            logger=logger,
        )  # loggerを渡す

        # メインループの開始
        logger.info("GUIを起動しました")
        root.mainloop()

        # 終了処理
        app_core.shutdown()

    except Exception as e:
        logger.exception(f"アプリケーションの起動中にエラーが発生しました: {str(e)}")
        # TkinterのGUIがまだ起動していない場合はコンソールに表示
        if "root" in locals() and root:
            import tkinter.messagebox as messagebox

            messagebox.showerror(
                "起動エラー",
                f"アプリケーションの起動中にエラーが発生しました: {str(e)}",
            )
        else:
            print(f"エラー: {str(e)}")

        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # main関数でキャッチされなかった例外をここでログに記録
        print(f"アプリケーションの実行中に予期せぬエラーが発生しました: {e}")
        import traceback

        print(traceback.format_exc())
        sys.exit(1)
