"""
競輪データ更新GUI
"""

import logging
import os
import sys
import threading  # これがない場合は追加
import tkinter as tk
import traceback  # 追加
from datetime import date, datetime, timedelta  # date を追加
from tkinter import filedialog, messagebox, ttk
from typing import List  # ★★★ List をインポート ★★★

from tkcalendar import DateEntry

# 自作モジュールのインポート
from gui.log_manager import LogManager
from gui.ui_builder import UIBuilder
from gui.update_manager import UpdateManager

# 親ディレクトリをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# config.py から Config クラスをインポート


class KeirinUpdaterGUI:
    """
    競輪データ更新GUIツール

    Tkinterを使用した競輪データ更新ツールのGUIインターフェース
    """

    def __init__(
        self, root, db_accessor=None, config=None, update_service=None, logger=None
    ):
        """
        初期化

        Args:
            root (tk.Tk): Tkinterのルートウィンドウ
            db_accessor (KeirinDataAccessor, optional): データベースアクセサインスタンス. Defaults to None.
            config (ConfigParser or similar, optional): 設定マネージャーインスタンス. Defaults to None.
            update_service (UpdateService, optional): 更新サービスインスタンス. Defaults to None.
            logger (logging.Logger, optional): ロガーインスタンス. Defaults to None.
        """
        self.root = root
        self.root.title("競輪データ更新ツール")
        self.root.geometry("800x650")  # ウィンドウサイズを拡大
        self.root.resizable(True, True)

        # ★★★ cancel_event の初期化を追加 ★★★
        self.cancel_event = threading.Event()

        # 外部から渡されたインスタンスを優先して使用
        self.logger = (
            logger or self._setup_internal_logger()
        )  # ロガーがない場合は内部で設定
        self.db_accessor = db_accessor
        self.config = config
        self.update_service = update_service

        # --- 既存の初期化処理で、外部インスタンスを使うように調整 ---
        # もしインスタンスが渡されなかった場合のフォールバックも考慮（単体テスト等）

        # ログ管理の初期化 (ロガーは確定しているので、LogManagerに渡す)
        # LogManager の初期化を self._init_managers の前に移動 or _init_loggingを先に呼ぶ
        # self._init_logging() # これは logger を設定するだけなので不要かも
        self.log_manager = LogManager(
            self, log_level=self.logger.level if self.logger else logging.INFO
        )
        # logger を LogManager のものに統一したほうが良いかも
        # self.logger = self.log_manager.logger
        self.logger.info("KeirinUpdaterGUI 初期化開始")

        # 設定クラスのインスタンス化 (config が渡されなかった場合のみ)
        if self.config is None:
            self.logger.warning(
                "Configインスタンスが渡されませんでした。内部で生成します。"
            )
            # config.py から Config クラスをインポート -> 既存コードを流用
            from config.config import Config

            self.config = Config(logger=self.logger)

        # 親ディレクトリの取得
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # 設定ファイルパス (config オブジェクトから取得できるか確認)
        # Configクラスが config_file 属性を持つことを期待
        self.config_file = getattr(
            self.config, "config_file", os.path.join(parent_dir, "config.ini")
        )

        # データベースパス（デフォルト値 -> config から読むように _init_variables で上書きされるはず）
        # db_accessor が渡されている場合は、そのパスを使う (KeirinDataAccessor がパス情報を保持しているか確認が必要)
        # KeirinDataAccessor が直接パスを保持していない場合、configから取得するなどの代替手段を検討
        if self.db_accessor:
            # KeirinDataAccessor が 'db_path' や 'get_db_path()' のような属性/メソッドを持つことを仮定
            # 持っていない場合は、configから読み込むか、固定値を設定する
            try:
                # KeirinDataAccessorからDBパス情報を取得する例
                # MySQL移行により、常に固定値を使用
                default_db_path = (
                    "MySQL Database (Connection managed by KeirinDataAccessor)"
                )
                self.logger.info(
                    f"KeirinDataAccessorからDBパス情報を取得試行: {default_db_path}"
                )
            except AttributeError:
                self.logger.warning(
                    "KeirinDataAccessorからDBパス情報を取得できませんでした。デフォルトパスを使用します。"
                )
                default_db_path = "MySQL Database (Connection managed by KeirinDataAccessor)"  # MySQL移行
        else:
            # self.db_accessor の初期化が正常に完了した場合のデフォルトパス設定
            # MySQL移行により、ローカルDBファイルパスの設定は不要
            self.logger.warning(
                "db_accessorが渡されませんでした。MySQL環境では外部DBアクセサが必要です。"
            )
            default_db_path = "MySQL Database (Connection required)"
        self.db_path = tk.StringVar(value=default_db_path)

        # 変数の初期化 (_init_variables で config の値を使う)
        self._init_variables()

        # マネージャーの初期化 (_init_managers で db や update_service を設定)
        # ただし、db と update_service は既に外部から渡されている可能性があるので、
        # _init_managers 内で上書きしないように注意が必要。
        self._init_managers()

        # 設定ファイルの読み込み (初期値をGUIに反映させるため)
        # config オブジェクトが既に読み込み済みかもしれないが、念のため呼ぶ
        # または _init_variables で config オブジェクトの値を使うように修正する
        # self._load_config() # _init_variables でカバーされるはず

        # UIの構築 (UIBuilder が self の変数やマネージャーを参照する)
        self.ui_builder = UIBuilder(self)  # UIBuilderは _init_managers より後に初期化
        self.ui_builder.build_ui()

        # プロトコル設定（ウィンドウ終了時の処理）
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

        # 初期メッセージの表示
        self._log_initial_messages()

        # スケジューラーを開始
        self._start_scheduler()

        # GUIの状態変数
        self.update_running = False
        self.deploy_running = False
        self.scheduler_running = False
        self.manual_update_running = False  # 手動更新実行中フラグ

        # スケジューラ関連
        # ... existing code ...

    def _setup_internal_logger(self):
        """外部からロガーが渡されなかった場合に内部で設定する"""
        # main.py の setup_application_logger と同様のロジックが良いかも
        # ここでは簡易的に設定
        internal_logger = logging.getLogger("KeirinUpdaterGUI_Internal")
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        internal_logger.addHandler(handler)
        internal_logger.setLevel(logging.INFO)
        internal_logger.warning(
            "外部ロガーが見つからないため、内部ロガーを使用します。"
        )
        return internal_logger

    def _init_managers(self):
        """各マネージャーの初期化 (外部インスタンスを優先)"""
        try:
            # ログマネージャーは __init__ で初期化済み
            self.logger.info("マネージャー初期化開始")

            # データベースの初期化 (db_accessor がなければエラーハンドリングが必要かもしれない)
            if self.db_accessor is None:
                # MySQLへの移行に伴い、db_accessorなしでGUIを動作させるのは困難なため、エラーまたは警告を出す
                self.logger.error(
                    "DBアクセサインスタンス(db_accessor)が渡されませんでした。GUIのデータベース関連機能は動作しません。"
                )
                # GUIの操作を制限するか、ユーザーに設定を促すメッセージを表示する等の対応が必要
                # ここでは、db_initializer の作成をスキップする
                self.db_initializer = None  # 初期化しない
            else:
                self.logger.info("外部DBアクセサインスタンスを使用します")
                # GUIのdb_path変数はMySQLの場合、接続情報の一部を表示する等、役割が変わる可能性がある
                # self.db_path.set(self.db_accessor.get_connection_string()) # 例: 接続文字列を取得するメソッドを想定

                # データベース初期化マネージャーの作成 (MySQLでは不要な可能性が高いが、既存コードに合わせて残すか検討)
                # DatabaseInitializer は SQLite/DuckDB のテーブル作成用なので、MySQLでは役割がない
                # self.db_initializer = DatabaseInitializer(self.db_accessor) # KeirinDataAccessor を渡すように変更
                self.db_initializer = None  # MySQL移行に伴い不要と判断
                self.logger.info(
                    "データベース初期化マネージャーはMySQL移行に伴い無効化されました。"
                )

            # 更新マネージャーの作成 (update_service がなければ生成)
            if self.update_service is None:
                self.logger.warning(
                    "UpdateServiceインスタンスが渡されませんでした。内部で生成します。"
                )
                # UpdateManager は UpdateService とは別物？ それともラッパー？
                # UpdateManagerが実際の更新処理を行うなら UpdateService は不要かも？
                # ここでは既存の UpdateManager を使う想定
                self.update_manager = UpdateManager(
                    self.db_accessor, self.logger, self, self.config
                )  # 引数を修正: db_accessor, logger, controller, config_manager
                self.logger.info("内部UpdateManager生成完了")
            else:
                # 外部の UpdateService を使う場合、UpdateManager はどうする？
                # UpdateManager はGUIとの連携用クラスかもしれない。
                # その場合、実際の処理は self.update_service を呼び出すように UpdateManager を改修する必要がある。
                # または、UpdateManager を使わず、直接 self.update_service を使う。
                # 一旦、既存の UpdateManager をそのまま使う（内部生成）ことにしておく。
                # TODO: UpdateManager と UpdateService の役割分担を明確にする必要あり。
                self.logger.info(
                    "外部UpdateServiceインスタンスを受け取りましたが、現時点では内部UpdateManagerを使用します。"
                )
                self.update_manager = UpdateManager(
                    self.db_accessor, self.logger, self, self.config
                )  # 引数を修正: db_accessor, logger, controller, config_manager

            # UI構築クラスは __init__ の最後に移動
            # self.ui_builder = UIBuilder(self)
            # self.logger.info("UI構築クラスの初期化が完了しました")

            # ★★★ データベースの状態確認・初期化 ★★★
            self.logger.info("データベースの状態を確認し、必要に応じて初期化します...")
            if self.db_initializer:  # db_initializer が None でない場合のみ実行
                init_success = self.db_initializer.initialize_database()
                if init_success:
                    self.logger.info("データベースの確認・初期化が完了しました。")
                else:
                    self.logger.error(
                        "データベースの確認・初期化に失敗しました。ログを確認してください。"
                    )
            else:
                self.logger.info(
                    "データベース初期化処理はスキップされました（MySQL環境）。"
                )

        except Exception as e:
            self.logger.error(f"マネージャーの初期化中にエラーが発生しました: {e}")
            import traceback

            self.logger.error(f"スタックトレース: {traceback.format_exc()}")
            # messagebox.showerror("エラー", f"マネージャーの初期化中にエラー: {e}") # mainスレッド以外から呼ぶとエラーになる可能性
            # エラー発生時は、GUIの起動自体が困難になる可能性があるため、ログへの記録を優先

    def _init_variables(self):
        """変数の初期化 (Config Manager の値を使用)"""
        # ロガーの初期化 (もし未初期化なら)
        if not hasattr(self, "logger"):
            self.logger = logging.getLogger(__name__)

        # --- ConfigManager の正しい getter を使うように修正 ---

        # 設定ファイルパス (ConfigManager が内部で管理するので、GUI側で保持する必要は薄いかも)
        # 必要なら get_config_path() 的なメソッドを ConfigManager に追加するか、ここで取得
        # self.config_file = self.config._get_config_path() # 直接アクセスは避けるべき
        # ここでは __init__ で設定されたものをそのまま使う

        # DBパス (MySQL移行により固定値を使用)
        default_db_path = "MySQL Database (Connection managed by KeirinDataAccessor)"
        db_path_value = self.config.get_value(
            "Database", "db_path", fallback=default_db_path
        )
        self.db_path = tk.StringVar(value=db_path_value)

        # 更新対象日 (セクション 'GUIState', オプション 'last_target_date' と仮定)
        default_target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        target_date_value = self.config.get_value(
            "GUIState", "last_target_date", fallback=default_target_date
        )
        self.target_date = tk.StringVar(value=target_date_value)

        # 更新フラグ (セクション 'Update', オプション名と仮定)
        self.update_winticket = tk.BooleanVar(
            value=self.config.get_boolean("Update", "update_winticket", fallback=True)
        )
        self.update_yenjoy = tk.BooleanVar(
            value=self.config.get_boolean("Update", "update_yenjoy", fallback=True)
        )

        # 更新ステップのフラグ (セクション 'UpdateSteps', オプション名と仮定)
        self.fetch_cups = tk.BooleanVar(
            value=self.config.get_boolean("UpdateSteps", "fetch_cups", fallback=True)
        )
        self.fetch_cup_details = tk.BooleanVar(
            value=self.config.get_boolean(
                "UpdateSteps", "fetch_cup_details", fallback=True
            )
        )
        self.fetch_race_data = tk.BooleanVar(
            value=self.config.get_boolean(
                "UpdateSteps", "fetch_race_data", fallback=True
            )
        )
        self.fetch_odds_data = tk.BooleanVar(
            value=self.config.get_boolean(
                "UpdateSteps", "fetch_odds_data", fallback=True
            )
        )
        self.fetch_yenjoy_results = tk.BooleanVar(
            value=self.config.get_boolean(
                "UpdateSteps", "fetch_yenjoy_results", fallback=True
            )
        )

        # 更新モード (セクション 'GUIState', オプション 'last_update_mode' と仮定)
        update_mode_value = self.config.get_value(
            "GUIState", "last_update_mode", fallback="single"
        )
        self.update_mode = tk.StringVar(value=update_mode_value)

        # 期間更新用 (セクション 'GUIState', オプション名と仮定)
        default_start_date_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        start_date_str = self.config.get_value(
            "GUIState", "last_start_date", fallback=default_start_date_str
        )
        try:
            self.start_date = datetime.strptime(start_date_str, "%Y%m%d").date()
        except (ValueError, TypeError):
            self.start_date = (datetime.now() - timedelta(days=1)).date()  # fallback

        default_end_date_str = datetime.now().strftime("%Y%m%d")
        end_date_str = self.config.get_value(
            "GUIState", "last_end_date", fallback=default_end_date_str
        )
        try:
            self.end_date = datetime.strptime(end_date_str, "%Y%m%d").date()
        except (ValueError, TypeError):
            self.end_date = datetime.now().date()  # fallback

        self.until_now = tk.BooleanVar(
            value=self.config.get_boolean("GUIState", "last_until_now", fallback=False)
        )

        # 自動更新設定 (セクション 'AutoUpdate', オプション名と仮定)
        self.auto_update = tk.BooleanVar(
            value=self.config.get_boolean("AutoUpdate", "enabled", fallback=False)
        )
        self.auto_update_interval = tk.IntVar(
            value=self.config.get_int("AutoUpdate", "interval_minutes", fallback=60)
        )
        self.auto_update_timer_id = None
        self.next_update_time = tk.StringVar(value="")  # 次回更新予定時刻

        # 更新中フラグ
        self.is_updating = False

        # ステータス表示用
        self.status_var = tk.StringVar(value="準備完了")

        # スケジューラー用：最後にチェックした時刻 (HH:MM)
        self._last_schedule_check_time_str = None

        # ★★★ 追加: 強制更新フラグ ★★★
        self.force_update_var = tk.BooleanVar(value=False)
        # ★★★ 追加ここまで ★★★

        # スケジューラー設定 (セクション 'Scheduler', オプション名と仮定)
        self.scheduler_enabled = tk.BooleanVar(
            value=self.config.get_boolean("Scheduler", "enabled", fallback=False)
        )
        self.schedule_time = tk.StringVar(
            value=self.config.get_value("Scheduler", "time", fallback="03:00")
        )

        # DuckDB出力パス (セクション 'Deployment', オプション 'frontend_db_path')
        default_duckdb_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "frontend.duckdb",
        )
        duckdb_path_value = self.config.get_value(
            "Deployment", "frontend_db_path", fallback=default_duckdb_path
        )
        self.duckdb_path = tk.StringVar(value=duckdb_path_value)

    def _log_initial_messages(self):
        """初期ログメッセージの表示"""
        self.log_manager.log("競輪データ更新ツールが起動しました。")
        # MySQL移行に伴い、db_pathは接続情報の一部を示すか、あるいは表示自体が不要になる可能性
        # self.log_manager.log(f"データベースファイル: {self.db_path.get()}")
        self.log_manager.log(f"更新対象日: {self.target_date.get()}")
        self.log_manager.log(f"設定ファイル: {self.config_file}")

        now = datetime.now()
        current_time_str = now.strftime("%H:%M")
        # デバッグ用に毎分ログ出力
        # self.logger.debug(f"スケジュールチェック実行: {current_time_str}")

        # 前回チェックした時刻と同じ分なら何もしない (秒単位のずれで複数回実行されるのを防ぐ)
        if (
            hasattr(self, "_last_schedule_check_time_str")
            and self._last_schedule_check_time_str == current_time_str
        ):
            # self.logger.debug(f"既に時刻 {current_time_str} はチェック済みのためスキップ")
            return

        self._last_schedule_check_time_str = (
            current_time_str  # 今回チェックした時刻を記録
        )

        try:
            schedule_list = self.config.get_schedule_list(fallback=[])
            for schedule in schedule_list:
                if not schedule.get("enabled", False):
                    continue  # 無効なスケジュールはスキップ

                schedule_time = schedule.get("time")
                is_enabled = schedule.get("enabled", False)  # enabled の値もログに出す
                # --- デバッグログ追加 -> INFOレベルに変更 ---
                self.logger.info(
                    f"Checking schedule: Time='{schedule_time}', Enabled={is_enabled}, CurrentTime='{current_time_str}'"
                )
                # ----------------------
                if is_enabled and schedule_time == current_time_str:
                    self.logger.info(f"スケジュール実行時刻です: {schedule_time}")
                    if self.is_updating:
                        self.logger.warning(
                            "現在、別の更新処理が実行中のため、スケジュールされた更新をスキップします。"
                        )
                    else:
                        steps = schedule.get("steps", "all")  # デフォルトは全ステップ
                        # GUIスレッドから直接実行せず、afterでメインスレッドに処理を依頼
                        self.root.after(0, self.run_scheduled_update, steps)
                    # 同じ時刻に複数スケジュールがあっても、最初の1つだけ実行（必要なら変更）
                    break
        except Exception as e:
            self.logger.error(
                f"スケジュールチェック中にエラーが発生: {e}", exc_info=True
            )

    def _create_new_database(self):
        """新しいデータベースを作成する"""
        # デフォルトのファイルパスを設定
        default_db_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "database",
            "keirin_data.sqlite",
        )

        # 新規データベースの保存場所を選択
        file_path = filedialog.asksaveasfilename(
            title="新規データベースの保存先を選択",
            initialdir=os.path.dirname(default_db_path),
            initialfile="keirin_data.sqlite",
            filetypes=[
                ("SQLiteデータベース", "*.sqlite *.db"),
                ("すべてのファイル", "*.*"),
            ],
        )

        if not file_path:
            # ユーザーがキャンセルした場合
            return

        # 確認ダイアログを表示
        confirm = messagebox.askyesno(
            "確認",
            f"次の場所に新しいデータベースを作成します。\n\n{file_path}\n\n続行しますか？",
        )

        if not confirm:
            return

        try:
            # データベースのパスを設定
            self.db_path.set(file_path)

            # 古いDBファイルがあれば確認
            if os.path.exists(file_path):
                overwrite = messagebox.askyesno(
                    "警告", f"ファイル '{file_path}' は既に存在します。上書きしますか？"
                )

                if not overwrite:
                    return

                try:
                    # バックアップ作成
                    backup_path = f"{file_path}.bak.{int(datetime.now().timestamp())}"
                    import shutil

                    shutil.copy2(file_path, backup_path)
                    self.log_manager.log(
                        f"既存のDBファイルをバックアップしました: {backup_path}"
                    )

                    # 既存ファイルを削除
                    os.remove(file_path)
                    self.log_manager.log(f"既存のDBファイルを削除しました: {file_path}")
                except Exception as e:
                    self.log_manager.log_error(
                        "バックアップ作成中にエラーが発生しました", e
                    )
                    return

            # データベース初期化処理
            self.log_manager.log(f"新規データベースの作成を開始します: {file_path}")

            # ステータスを更新
            self.status_var.set("データベース作成中...")
            self.ui_builder.update_progress(True, "データベース作成中...")

            # 処理を別スレッドで実行
            import threading

            thread = threading.Thread(
                target=self._create_database_thread, args=(file_path,)
            )
            thread.daemon = True
            thread.start()

        except Exception as e:
            self.log_manager.log_error("データベース作成中にエラーが発生しました", e)
            self.status_var.set("エラー")
            self.ui_builder.update_progress(False, "エラー")

    def _create_database_thread(self, db_path):
        """データベース作成処理を別スレッドで実行"""
        try:
            # DBイニシャライザを使用してデータベースを作成
            # MySQL移行後はGUIからのDB作成は基本的に行わない想定
            self.logger.warning(
                "MySQL環境ではGUIからのデータベース作成はサポートされていません。"
            )
            self.root.after(
                0,
                lambda: self._database_creation_completed(
                    False,
                    "MySQL環境ではGUIからのデータベース作成はサポートされていません。",
                ),
            )

        except Exception:  # Removed 'as e' as it's unused
            # エラー発生時
            # Kept the original lambda err_msg=str(e) as 'e' is not in this scope anymore after removing 'as e'.
            # This part might need to be re-evaluated if a generic error message is not desired.
            self.root.after(
                0,
                lambda: self._database_creation_completed(
                    False, "エラー: データベース作成中に予期せぬエラーが発生しました。"
                ),
            )

    def _database_creation_completed(self, success, message):
        """データベース作成完了後の処理（メインスレッドから呼ばれる）"""
        if success:
            self.log_manager.log(message)
            messagebox.showinfo("完了", message)
        else:
            self.log_manager.log(message, level="ERROR")
            messagebox.showerror("エラー", message)

        # UIの状態を戻す
        self.status_var.set("準備完了")
        self.ui_builder.update_progress(False, "準備完了")

        # 設定を保存
        self._save_config()

    def _set_date(self, days_ago):
        """指定日数前の日付を設定"""
        date = datetime.now() - timedelta(days=days_ago)

        # date_entryがDateEntryウィジェットの場合のみset_dateを呼び出す
        if hasattr(self.ui_builder, "date_entry"):
            if hasattr(self.ui_builder.date_entry, "set_date"):
                self.ui_builder.date_entry.set_date(date)

        # 日付文字列は常に設定
        date_str = date.strftime("%Y%m%d")
        self.target_date.set(date_str)
        self.log_manager.log(f"更新対象日を変更: {date_str}")

    def _toggle_update_mode(self):
        """更新モードの切り替え処理"""
        mode = self.update_mode.get()

        # すべてのモードフレームを非表示にする
        self.ui_builder.single_date_frame.pack_forget()
        self.ui_builder.period_frame.pack_forget()

        if mode == "single":
            # 単一日付モード
            self.ui_builder.single_date_frame.pack(
                fill=tk.X, pady=5, after=self.ui_builder.mode_selection_frame
            )
            self.log_manager.log("単一日付更新モードに設定しました")

            # DateEntryウィジェットがある場合のみget_dateを呼び出す
            if hasattr(self.ui_builder, "date_entry"):
                date_entry = self.ui_builder.date_entry
                # Entryウィジェットの場合は文字列を直接取得
                if isinstance(date_entry, ttk.Entry):
                    date_str = self.target_date.get()
                # DateEntryウィジェットの場合はget_dateメソッドを使用
                elif hasattr(date_entry, "get_date"):
                    try:
                        selected_date = date_entry.get_date()
                        date_str = selected_date.strftime("%Y%m%d")
                        self.target_date.set(date_str)
                    except Exception as e:
                        self.log_manager.log_error(
                            "日付取得中にエラーが発生しました", e
                        )

        elif mode == "period":
            # 期間指定モード
            self.ui_builder.period_frame.pack(
                fill=tk.X, pady=5, after=self.ui_builder.mode_selection_frame
            )
            self.log_manager.log("期間指定更新モードに設定しました")

            # 期間の開始日と終了日を設定（安全に実行するため例外処理を追加）
            try:
                if hasattr(self.ui_builder, "start_date_entry") and hasattr(
                    self.ui_builder.start_date_entry, "get_date"
                ):
                    self.start_date = self.ui_builder.start_date_entry.get_date()

                if self.until_now.get():
                    self.end_date = datetime.now().date()
                    self.log_manager.log("終了日を現在に設定しました")
                else:
                    if hasattr(self.ui_builder, "end_date_entry") and hasattr(
                        self.ui_builder.end_date_entry, "get_date"
                    ):
                        self.end_date = self.ui_builder.end_date_entry.get_date()
            except Exception as e:
                self.log_manager.log_error("日付設定中にエラーが発生しました", e)

        elif mode == "all":
            # 全期間モード (過去30日)
            self.log_manager.log("全期間更新モード（過去30日）に設定しました")

    def _toggle_until_now(self):
        """「現在まで」オプションの切り替え処理"""
        if self.until_now.get():
            # "現在まで"が有効な場合、終了日の入力を無効化
            self.ui_builder.end_date_entry.config(state="disabled")
            self.log_manager.log("終了日を現在に設定しました")
        else:
            # "現在まで"が無効な場合、終了日の入力を有効化
            self.ui_builder.end_date_entry.config(state="normal")
            self.log_manager.log("終了日を手動設定に変更しました")

    def _on_closing(self):
        """ウィンドウ閉じる際の処理"""
        try:
            # 更新中の場合は確認
            if self.is_updating:
                response = messagebox.askyesno(
                    "確認",
                    "更新処理が実行中です。\n終了すると更新処理は中断されます。\n本当に終了しますか？",
                )
                if not response:
                    return

                # 更新処理を中止
                self.is_updating = False
                self.log_manager.log("更新処理を中断して終了します", level="WARNING")

            # 自動更新タイマーの停止 -> スケジューラーの停止に変更
            # self._cancel_auto_update()
            self._stop_scheduler()

            # 設定の保存
            self._save_config()

            # 接続やリソースの解放
            self._cleanup_resources()

            # ロガーのシャットダウン
            if hasattr(self, "log_manager"):
                self.log_manager.shutdown()

            # 更新スレッドが終了するまで待機
            self._wait_for_threads()

            # ウィンドウを閉じる
            self.root.destroy()

        except Exception as e:
            # エラーログを出力
            if hasattr(self, "log_manager"):
                self.log_manager.log_error("終了処理中にエラーが発生しました", e)

            # 強制終了
            if hasattr(self, "root"):
                self.root.destroy()

    def _wait_for_threads(self):
        """更新スレッドの終了を待つ"""
        try:
            if hasattr(self, "update_manager") and hasattr(
                self.update_manager, "update_thread"
            ):
                update_thread = self.update_manager.update_thread
                if update_thread and update_thread.is_alive():
                    # スレッドが終了するまで最大3秒待機
                    self.log_manager.log(
                        "更新スレッドの終了を待機しています...", level="INFO"
                    )
                    for _ in range(30):
                        if not update_thread.is_alive():
                            self.log_manager.log(
                                "更新スレッドが終了しました", level="INFO"
                            )
                            break
                        # GUIを更新しながら待機
                        self.root.update_idletasks()
                        self.root.after(100)

                    # それでも終了しない場合は警告
                    if update_thread.is_alive():
                        self.log_manager.log(
                            "更新スレッドが終了しませんでした。強制終了します。",
                            level="WARNING",
                        )
        except Exception as e:
            if hasattr(self, "log_manager"):
                self.log_manager.log_error("スレッド待機中にエラーが発生しました", e)

    def _cleanup_resources(self):
        """リソースの解放"""
        try:
            # データベース接続の解放
            if hasattr(self, "update_manager"):
                if (
                    hasattr(self.update_manager, "data_accessor")
                    and self.update_manager.data_accessor
                ):
                    try:
                        self.update_manager.data_accessor.rollback()  # 未コミットのトランザクションをロールバック
                        self.log_manager.log(
                            "未完了のトランザクションをロールバックしました",
                            level="INFO",
                        )
                    except Exception as e:
                        self.log_manager.log_error(
                            "トランザクションのロールバック中にエラーが発生しました", e
                        )

                # APIクライアントのクリーンアップ
                if hasattr(self.update_manager, "winticket_api"):
                    try:
                        if hasattr(self.update_manager.winticket_api, "cleanup"):
                            self.update_manager.winticket_api.cleanup()
                    except Exception as e:
                        self.log_manager.log_error(
                            "WinticketAPIのクリーンアップ中にエラーが発生しました", e
                        )

                if hasattr(self.update_manager, "yenjoy_api"):
                    try:
                        if hasattr(self.update_manager.yenjoy_api, "cleanup"):
                            self.update_manager.yenjoy_api.cleanup()
                    except Exception as e:
                        self.log_manager.log_error(
                            "YenjoyAPIのクリーンアップ中にエラーが発生しました", e
                        )
        except Exception as e:
            if hasattr(self, "log_manager"):
                self.log_manager.log_error("リソースの解放中にエラーが発生しました", e)

    def _check_and_create_default_db(self):
        """デフォルトのデータベースファイルが存在しない場合、ユーザーに作成を促す"""
        db_path = self.db_path.get()
        if not os.path.exists(db_path):
            self.logger.info("データベースファイルを指定されたパスに作成します。")
            # messagebox関連のコード

    def _start_update(self):
        """更新処理の開始 (タブに応じて処理を分岐)"""
        if self.update_running:
            messagebox.showwarning("実行中", "更新処理が既に実行中です。")
            return

        # 強制更新フラグを取得
        force = self.force_update_var.get()

        # 現在選択されているタブのインデックスを取得
        try:
            current_tab_index = self.tab_control.index(self.tab_control.select())
        except tk.TclError:
            messagebox.showerror("エラー", "更新タブの取得に失敗しました。")
            return

        # 選択されたステップを取得
        selected_steps = self._get_selected_steps()
        if not selected_steps:
            messagebox.showwarning(
                "選択なし", "更新するステップを1つ以上選択してください。"
            )
            return

        # タブインデックスに応じて処理を分岐
        if current_tab_index == 0:  # 単一日更新タブ
            if not hasattr(self, "target_date") or not self.target_date:
                messagebox.showerror(
                    "エラー",
                    "単一日更新の日付がセットされていません。「日付セット」ボタンを押してください。",
                )
                return

            target_date_to_pass = self.target_date
            if isinstance(self.target_date, date):
                target_date_to_pass = self.target_date.strftime("%Y%m%d")

            self._update_single_day(
                target_date_str=target_date_to_pass, steps=selected_steps, force=force
            )

        elif current_tab_index == 1:  # 期間更新タブ
            if (
                not hasattr(self, "start_date")
                or not self.start_date
                or not hasattr(self, "end_date")
                or not self.end_date
            ):
                messagebox.showerror(
                    "エラー",
                    "期間更新の日付がセットされていません。「期間セット」ボタンを押してください。",
                )
                return

            start_date_to_pass = self.start_date
            if isinstance(self.start_date, date):
                start_date_to_pass = self.start_date.strftime("%Y%m%d")

            end_date_to_pass = self.end_date
            if isinstance(self.end_date, date):
                end_date_to_pass = self.end_date.strftime("%Y%m%d")

            self._update_period(
                start_date_str=start_date_to_pass,
                end_date_str=end_date_to_pass,
                steps=selected_steps,
                force=force,
            )

        elif current_tab_index == 2:  # 全期間更新タブ
            # 全期間更新のロジックを呼び出す (日付範囲は内部で決定)
            self._update_all(steps=selected_steps, force=force)

        else:
            messagebox.showerror("エラー", "不明な更新タブです。")

    def _update_single_day(
        self, target_date_str: str, steps: List[str], force: bool = False
    ):  # ★★★ 引数に force を追加 ★★★
        """単一日更新処理を開始 (引数で日付を受け取る)"""
        try:
            # target_date_str ('YYYYMMDD') を datetime オブジェクトに変換
            target_date_dt = datetime.strptime(target_date_str, "%Y%m%d")
            # ログや表示用に 'YYYY-MM-DD' 形式に変換
            display_date_str = target_date_dt.strftime("%Y-%m-%d")

            # 実行確認 (表示用にフォーマットした日付を使用)
            if not messagebox.askyesno(
                "確認",
                f"{display_date_str} のデータを更新しますか？\nステップ: {', '.join(steps)}\n強制更新: {'有効' if force else '無効'}",
                parent=self.root,
            ):
                return

            self.update_running = True
            self._disable_relevant_buttons()
            self.log_manager.clear_log()
            # ログには YYYYMMDD 形式を使用
            self.logger.info(
                f"単一日更新を開始します: {target_date_str}, ステップ: {', '.join(steps)}, 強制: {force}"
            )

            # --- 更新処理を別スレッドで実行 ---
            # _run_update_thread には 'YYYY-MM-DD' 形式で渡す
            thread = threading.Thread(
                target=self._run_update_thread,
                args=(
                    display_date_str,  # 開始日
                    display_date_str,  # 終了日 (単一日なので同じ)
                    steps,
                    force,
                ),
                daemon=True,
            )
            thread.start()

        except ValueError:
            messagebox.showerror(
                "エラー",
                f"日付の形式が無効です: {target_date_str} (YYYYMMDD形式である必要があります)",
            )
        except Exception as e:
            self.logger.error(f"単一日更新の開始中にエラー: {e}", exc_info=True)
            messagebox.showerror("エラー", f"更新開始中にエラーが発生しました: {e}")
            self._enable_relevant_buttons()
            self.update_running = False

    def _update_period(
        self,
        start_date_str: str,
        end_date_str: str,
        steps: List[str],
        force: bool = False,
    ):  # ★★★ 修正: 引数で日付とステップを受け取るように変更 ★★★
        """期間更新処理を開始 (引数で日付を受け取る)"""
        try:
            # 'YYYYMMDD' 形式の文字列を datetime オブジェクトに変換
            start_dt = datetime.strptime(start_date_str, "%Y%m%d")
            end_dt = datetime.strptime(end_date_str, "%Y%m%d")

            # 開始日 <= 終了日 かチェック
            if start_dt > end_dt:
                messagebox.showerror(
                    "日付エラー", "開始日は終了日より後の日付にできません。"
                )
                return

            # ログや表示用に 'YYYY-MM-DD' 形式に変換
            display_start_date = start_dt.strftime("%Y-%m-%d")
            display_end_date = end_dt.strftime("%Y-%m-%d")

            # 実行確認
            if not messagebox.askyesno(
                f"{display_start_date} から {display_end_date} までのデータを更新しますか？\nステップ: {', '.join(steps)}\n強制更新: {'有効' if force else '無効'}",
                parent=self.root,
            ):
                return

            self.update_running = True
            self._disable_relevant_buttons()
            self.log_manager.clear_log()
            # ログには YYYYMMDD 形式を使用
            self.logger.info(
                f"期間更新を開始します: {start_date_str} - {end_date_str}, ステップ: {', '.join(steps)}, 強制: {force}"
            )

            # --- 更新処理を別スレッドで実行 ---
            # _run_update_thread には 'YYYY-MM-DD' 形式で渡す
            thread = threading.Thread(
                target=self._run_update_thread,
                args=(
                    display_start_date,  # 開始日
                    display_end_date,  # 終了日
                    steps,
                    force,  # ★★★ UpdateService に force フラグを渡す ★★★
                ),
                daemon=True,
            )
            thread.start()

        except ValueError:
            messagebox.showerror(
                "エラー",
                f"日付の形式が無効です: 開始='{start_date_str}', 終了='{end_date_str}' (YYYYMMDD形式である必要があります)",
            )
        except Exception as e:
            self.logger.error(f"期間更新の開始中にエラー: {e}", exc_info=True)
            messagebox.showerror("エラー", f"更新開始中にエラーが発生しました: {e}")
            self._enable_relevant_buttons()
            self.update_running = False

    def _update_all(
        self, steps: List[str], force: bool = False
    ):  # ★★★ 修正: 引数でステップとforceフラグを受け取るように変更 ★★★
        """全期間更新処理を開始"""
        # データベース内の最も古い日付と最新の日付を取得
        try:
            # DBから日付範囲を取得
            min_max_query = "SELECT MIN(date), MAX(date) FROM cups"
            result = self.db_accessor.execute_query(min_max_query, fetch_one=True)
            if result and result[0] and result[1]:
                all_start_date = result[0]  # 'YYYY-MM-DD' 形式のはず
                all_end_date = result[1]  # 'YYYY-MM-DD' 形式のはず
                # 念のため現在日付と比較して新しい方を採用
                current_date_str = datetime.now().strftime("%Y-%m-%d")
                all_end_date = max(all_end_date, current_date_str)
            else:
                # データがない場合のフォールバック
                all_start_date = "2020-01-01"  # または適切なデフォルト値
                all_end_date = datetime.now().strftime("%Y-%m-%d")
                self.logger.warning(
                    "DBから日付範囲を取得できませんでした。デフォルト範囲を使用します。"
                )

            # 実行確認
            if not messagebox.askyesno(
                "確認",
                f"全期間 ({all_start_date} から {all_end_date} まで) のデータを更新しますか？\nステップ: {', '.join(steps)}\n強制更新: {'有効' if force else '無効'}",
                parent=self.root,
            ):
                return

            self.update_running = True
            self._disable_relevant_buttons()
            self.log_manager.clear_log()
            self.logger.info(
                f"全期間更新を開始します: {all_start_date} - {all_end_date}, ステップ: {', '.join(steps)}, 強制: {force}"
            )

            # --- 更新処理を別スレッドで実行 ---
            # _run_update_thread には 'YYYY-MM-DD' 形式で渡す
            thread = threading.Thread(
                target=self._run_update_thread,
                args=(all_start_date, all_end_date, steps, force),  # 開始日  # 終了日
                daemon=True,
            )
            thread.start()

        except Exception as e:
            self.logger.error(f"全期間更新の開始中にエラー: {e}", exc_info=True)
            messagebox.showerror("エラー", f"更新開始中にエラーが発生しました: {e}")
            self._enable_relevant_buttons()
            self.update_running = False

    def _run_update_thread(
        self, start_date, end_date, steps, force=False
    ):  # ★★★ 引数に force を追加 ★★★
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"更新スレッド開始 (Thread-{thread_id} (_run_update_thread)): "
            f"{start_date} - {end_date}, ステップ: {steps}, 強制: {force}"
        )
        self.update_running = True
        # self.ui_builder.update_status_display( # 変更前
        #     f"更新処理中... (期間: {start_date} - {end_date}, ステップ: {steps}, 強制: {force})",
        #     "blue",
        # )
        if hasattr(self, "update_progress_label"):  # 変更後
            self.update_progress_label.config(
                text=f"更新処理中... (期間: {start_date} - {end_date}, ステップ: {steps}, 強制: {force})",
                foreground="blue",
            )
        else:
            self.status_var.set(
                f"更新処理中... (期間: {start_date} - {end_date}, ステップ: {steps}, 強制: {force})"
            )

        self._disable_relevant_buttons()

        success = False
        results_dict = {}
        error_message = None

        try:
            # サービスメソッドを呼び出す際に force_update_all を使用
            success, results_dict = self.update_service.update_period_step_by_step(
                start_date,
                end_date,
                steps,
                force_update_all=force,  # ★ ここを force_update_all に変更
                # callback=progress_callback  # callback引数が存在すれば渡す
            )
            if not success:
                self.logger.error(
                    f"更新処理失敗 (Thread-{thread_id}): {results_dict.get('error', '詳細不明')}"
                )
                error_message = (
                    results_dict.get("error")
                    or results_dict.get("messages", ["更新に失敗しました"])[0]
                )
            else:
                self.logger.info(f"更新処理成功 (Thread-{thread_id})")

        except Exception as e:
            self.logger.error(
                f"更新スレッドで予期せぬエラー: {str(e)}\n{traceback.format_exc()}",
                exc_info=True,
            )  # traceback.format_exc() を使用
            error_message = f"予期せぬエラー: {str(e)}"  # str(e) を使用
            self.update_running = False
            # self.ui_builder.update_status_display( # 変更前
            #     f"更新処理中にエラーが発生しました: {error_message}", "red"
            # )
            if hasattr(self, "update_progress_label"):  # 変更後
                self.update_progress_label.config(
                    text=f"更新処理中にエラーが発生しました: {error_message}",
                    foreground="red",
                )
            else:
                self.status_var.set(
                    f"更新処理中にエラーが発生しました: {error_message}"
                )

        finally:
            self.root.after(
                0, self._update_gui_after_thread, success, results_dict, error_message
            )

    def _update_gui_after_thread(self, success, results, error_message):
        """更新スレッド完了後にGUIを更新"""
        self.logger.info("GUI更新処理（スレッド完了後）を開始")
        # ... (既存のログ出力、プログレスバー、ボタン有効化処理) ...

        if error_message:
            messagebox.showerror("更新エラー", error_message)
        elif success:
            messagebox.showinfo("更新完了", "データ更新処理が完了しました。")
            # ここで結果 results をログやステータスバーに表示しても良い
        else:
            # results にステップごとの成否が含まれているはず
            fail_messages = []
            if isinstance(results, dict) and "steps" in results:
                for step, res in results["steps"].items():
                    if not res.get("success"):
                        fail_messages.append(
                            f"ステップ {step}: {res.get('message', '失敗')}"
                        )
            if fail_messages:
                messagebox.showwarning(
                    "更新一部失敗",
                    "データ更新処理中に一部失敗しました。\n\n"
                    + "\n".join(fail_messages)
                    + "\n\n詳細はログを確認してください。",
                )
            else:
                messagebox.showwarning(
                    "更新失敗",
                    "データ更新処理が失敗しました。詳細はログを確認してください。",
                )

        self.update_running = False
        self._enable_relevant_buttons()
        self.logger.info("GUI更新処理（スレッド完了後）を終了")

    def _get_selected_steps(self) -> List[str]:
        """選択されている更新ステップのリストを取得"""
        steps = []
        # step_name と self の BooleanVar のマッピング
        step_map = {
            "step1": self.fetch_cups,  # 開催
            "step2": self.fetch_cup_details,  # 詳細
            "step3": self.fetch_race_data,  # レース
            "step4": self.fetch_odds_data,  # オッズ
            "step5": self.fetch_yenjoy_results,  # Yenjoy/結果
        }

        # 各ステップの BooleanVar の値を確認
        for step_name, step_var in step_map.items():
            # BooleanVar が実際に存在し、値が True かどうかを確認
            if isinstance(step_var, tk.BooleanVar) and step_var.get():
                steps.append(step_name)

        if not steps:
            self.logger.warning("更新ステップが選択されていません。")

        return steps

    def _cancel_update(self):
        """更新処理のキャンセル"""
        if not self.is_updating:
            self.logger.warning("キャンセルする更新処理がありません")
            return

        # キャンセル確認
        if not messagebox.askyesno("確認", "更新処理をキャンセルしますか？"):
            return

        # 更新処理のキャンセル
        self.logger.info("更新処理のキャンセルを開始します")

        if hasattr(self, "update_manager"):
            # キャンセル処理
            cancel_success = self.update_manager.cancel_update()

            if cancel_success:
                self.logger.info("更新処理のキャンセルを要求しました")
                self.status_var.set("キャンセル中...")
            else:
                self.logger.warning("キャンセル可能な更新処理がありませんでした")
                self.is_updating = False
                self.ui_builder.enable_controls()
                self.ui_builder.update_progress(False, "キャンセル失敗")
        else:
            self.logger.error("更新マネージャーが初期化されていません")
            self.is_updating = False
            self.ui_builder.enable_controls()

    def _save_config(self):
        """設定をファイルに保存する (Config クラスを使用)"""
        try:
            # GUI の状態を Config インスタンスに反映
            # MySQL移行に伴い、db_pathの保存は必須ではないかもしれない
            # self.config.db_path = self.db_path.get()
            self.config.update_winticket = self.update_winticket.get()
            self.config.update_yenjoy = self.update_yenjoy.get()
            self.config.fetch_cups = self.fetch_cups.get()
            self.config.fetch_cup_details = self.fetch_cup_details.get()
            self.config.fetch_race_data = self.fetch_race_data.get()
            self.config.fetch_odds_data = self.fetch_odds_data.get()
            self.config.fetch_yenjoy_results = self.fetch_yenjoy_results.get()
            self.config.auto_update = self.auto_update.get()
            self.config.auto_update_interval = self.auto_update_interval.get()

            # GUI 固有設定
            self.config.last_update_mode = self.update_mode.get()
            self.config.last_target_date = self.target_date.get()  # YYYYMMDD 文字列
            # 期間日付は DateEntry から取得して YYYYMMDD 文字列に変換
            if (
                hasattr(self.ui_builder, "start_date_entry")
                and self.ui_builder.start_date_entry.winfo_exists()
            ):
                try:
                    self.config.last_start_date = (
                        self.ui_builder.start_date_entry.get_date().strftime("%Y%m%d")
                    )
                except Exception:
                    pass  # エラー時は更新しない
            if (
                hasattr(self.ui_builder, "end_date_entry")
                and self.ui_builder.end_date_entry.winfo_exists()
            ):
                try:
                    # 「今日まで」チェックボックスの状態も考慮
                    if self.until_now.get():
                        self.config.last_end_date = datetime.now().strftime("%Y%m%d")
                    else:
                        self.config.last_end_date = (
                            self.ui_builder.end_date_entry.get_date().strftime("%Y%m%d")
                        )
                except Exception:
                    pass  # エラー時は更新しない
            self.config.last_until_now = self.until_now.get()

            # Config クラスの save メソッドを呼び出し -> ConfigManager の save_config を呼ぶ
            # self.config.save()
            self.config.save_config()

            self.logger.info("設定を保存しました")

        except Exception as e:
            self.logger.error(f"設定の保存中にエラーが発生しました: {e}", exc_info=True)
            messagebox.showerror("エラー", f"設定の保存中にエラーが発生しました: {e}")

    def _load_config(self):
        """設定をファイルから読み込む (Config クラスが初期化時に実行)"""
        # Config クラスのインスタンス化時に load() が呼ばれるため、
        # ここでは明示的な再読み込みは行わない (必要であれば self.config.load() を呼ぶ)
        # GUI変数の初期化は _init_variables で行う
        self.logger.debug("Config クラスのインスタンスから設定値を反映済み")
        pass  # 特に処理は不要

    def _update_button(self):
        """更新ボタン押下時の処理"""
        try:
            self.logger.log("更新ボタンがクリックされました", level=logging.INFO)

            # ログウィンドウをクリア
            # self.logger.clear_log() # clear_log は self.log_manager にある想定
            if hasattr(self, "log_manager") and hasattr(self.log_manager, "clear_log"):
                self.log_manager.clear_log()

            # 現在の日付を取得
            date_str = (
                self.target_date.get()
            )  # ui_builder.date_var ではなく target_date を使用
            self.logger.log(f"対象日付: {date_str}", level=logging.INFO)

            # データベースパスの検証 (MySQLでは直接のパス検証はしない)
            # db_path = self.db_path.get()
            # self.logger.log(f"データベースパス: {db_path}", level=logging.INFO)

            # 更新モードの確認
            update_mode = self.update_mode.get()  # .get() を追加
            self.logger.log(f"更新モード: {update_mode}", level=logging.INFO)

            # 更新APIの設定を確認
            self.logger.log(
                f"Winticket API: {'有効' if self.update_winticket.get() else '無効'}",
                level=logging.INFO,
            )
            self.logger.log(
                f"Yenjoy API: {'有効' if self.update_yenjoy.get() else '無効'}",
                level=logging.INFO,
            )

            # 入力検証 (UpdateManagerのvalidate_inputsがMySQL環境で適切か確認)
            # if not self.update_manager.validate_inputs(date_str, db_path):
            if not self.update_manager or not self.update_manager.validate_inputs(
                date_str, None
            ):  # db_pathは渡さない
                self.logger.log(
                    "入力値の検証に失敗しました。更新を中止します。",
                    level=logging.ERROR,
                )
                return

            # 処理中の状態に遷移
            self.ui_builder.update_progress(True, "更新中...")
            self.is_updating = True

            # 日付形式の変換（必要な場合）
            if update_mode == "period":
                # 期間指定モードの場合、開始日と終了日を取得
                # start_date = self.ui_builder.period_start_date # UIBuilderから直接取得しない
                # end_date = self.ui_builder.period_end_date
                start_date = (
                    self.start_date
                )  # _set_period_dates でセットされたものを使用
                end_date = self.end_date
                self.logger.log(
                    f"期間: {start_date} から {end_date} まで",  # YYYYMMDD形式のはず
                    level=logging.INFO,
                )

                # 更新処理を開始 (UpdateManagerのstart_updateがMySQL環境で適切か確認)
                # self.update_manager.start_update(
                #     update_mode, date_str, start_date, end_date
                # ) # date_str は不要なはず
                if self.update_manager:
                    self.update_manager.start_update(
                        update_mode, None, start_date, end_date
                    )
            else:
                # 単一日付または全期間モードの場合
                self.logger.log(
                    f"{update_mode}モードで更新を開始します: {date_str}",
                    level=logging.INFO,
                )

                # 更新処理を開始
                if self.update_manager:
                    self.update_manager.start_update(update_mode, date_str)

            # 設定を保存
            self._save_config()

        except Exception as e:
            self.logger.log_error("更新処理の開始中にエラーが発生しました", e)
            if hasattr(self, "ui_builder"):  # ui_builder が存在するか確認
                self.ui_builder.update_progress(False, "エラー")
            self.is_updating = False

    def _show_calendar(self):
        """日付選択用カレンダーを表示"""
        try:
            # カレンダーウィンドウの作成
            calendar_window = tk.Toplevel(self.root)
            calendar_window.title("日付選択")
            calendar_window.geometry("300x300")
            calendar_window.resizable(False, False)

            # モーダルウィンドウに設定
            calendar_window.transient(self.root)
            calendar_window.grab_set()

            # 現在の日付または設定された日付を取得
            date_str = self.target_date.get()
            try:
                current_date = datetime.strptime(date_str, "%Y%m%d").date()
            except Exception:  # Bare except
                current_date = datetime.now().date() - timedelta(days=1)

            # カレンダーの作成
            cal = DateEntry(
                calendar_window,
                width=12,
                background="darkblue",
                foreground="white",
                borderwidth=2,
                date_pattern="yyyy/mm/dd",
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                locale="ja_JP",
            )
            cal.pack(padx=10, pady=10)

            # ボタンフレーム
            btn_frame = ttk.Frame(calendar_window)
            btn_frame.pack(fill=tk.X, padx=10, pady=10)

            # 決定ボタン
            def on_select():
                selected_date = cal.get_date()
                self.target_date.set(selected_date.strftime("%Y%m%d"))
                self.logger.log(
                    f"日付を選択しました: {selected_date.strftime('%Y/%m/%d')}"
                )
                calendar_window.destroy()

            ttk.Button(btn_frame, text="決定", command=on_select).pack(
                side=tk.RIGHT, padx=5
            )

            # キャンセルボタン
            ttk.Button(
                btn_frame, text="キャンセル", command=calendar_window.destroy
            ).pack(side=tk.RIGHT, padx=5)

            # クイック選択ボタン
            quick_frame = ttk.LabelFrame(calendar_window, text="クイック選択")
            quick_frame.pack(fill=tk.X, padx=10, pady=10)

            quick_btn_frame = ttk.Frame(quick_frame)
            quick_btn_frame.pack(fill=tk.X, padx=5, pady=5)

            # 今日
            def set_today():
                cal.set_date(datetime.now().date())

            ttk.Button(quick_btn_frame, text="今日", command=set_today).pack(
                side=tk.LEFT, padx=2
            )

            # 昨日
            def set_yesterday():
                cal.set_date(datetime.now().date() - timedelta(days=1))

            ttk.Button(quick_btn_frame, text="昨日", command=set_yesterday).pack(
                side=tk.LEFT, padx=2
            )

            # 一週間前
            def set_last_week():
                cal.set_date(datetime.now().date() - timedelta(days=7))

            ttk.Button(quick_btn_frame, text="1週間前", command=set_last_week).pack(
                side=tk.LEFT, padx=2
            )

            # ウィンドウの中央配置
            calendar_window.update_idletasks()
            width = calendar_window.winfo_width()
            height = calendar_window.winfo_height()
            x = (self.root.winfo_width() - width) // 2 + self.root.winfo_x()
            y = (self.root.winfo_height() - height) // 2 + self.root.winfo_y()
            calendar_window.geometry(f"{width}x{height}+{x}+{y}")

            # ウィンドウがフォーカスを失った場合も閉じる
            calendar_window.focus_set()
            calendar_window.wait_window()

        except Exception as e:
            self.logger.log_error("カレンダー表示中にエラーが発生しました", e)
            messagebox.showerror(
                "エラー", f"カレンダー表示中にエラーが発生しました: {str(e)}"
            )

    def _create_menu(self):
        """メニューバーの作成"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # ファイルメニュー
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="ファイル", menu=file_menu)
        file_menu.add_command(label="設定", command=self._open_settings)
        file_menu.add_separator()
        file_menu.add_command(label="終了", command=self.root.quit)

        # データベースメニュー
        db_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="データベース", menu=db_menu)
        db_menu.add_command(label="初期化", command=self._initialize_database)
        db_menu.add_command(label="バックアップ", command=self._backup_database)
        db_menu.add_command(label="復元", command=self._restore_database)

        # ヘルプメニュー
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="ヘルプ", menu=help_menu)
        help_menu.add_command(label="使い方", command=self._show_help)
        help_menu.add_command(label="バージョン情報", command=self._show_about)

    def _create_update_options(self, parent):
        """更新オプションフレームの作成 (UIBuilderから呼び出される)"""
        options_frame = ttk.LabelFrame(parent, text="更新オプション")
        # pack ではなく grid や他のジオメトリマネージャを使う可能性もある
        # 呼び出し元 (UIBuilder) の実装に依存する
        # ここでは pack を使うと仮定
        options_frame.pack(fill=tk.X, padx=5, pady=5)

        # --- データソース選択 --- (既存のコードを想定)
        source_frame = ttk.Frame(options_frame)
        source_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(source_frame, text="データソース:").pack(side=tk.LEFT, padx=5)
        # Winticketチェックボックス (self.update_winticket は _init_variables で初期化済み)
        winticket_cb = ttk.Checkbutton(
            source_frame,
            text="Winticket",
            variable=self.update_winticket,
            command=self._update_options,
        )
        winticket_cb.pack(side=tk.LEFT, padx=5)
        # Yenjoyチェックボックス (self.update_yenjoy は _init_variables で初期化済み)
        yenjoy_cb = ttk.Checkbutton(
            source_frame,
            text="Yenjoy",
            variable=self.update_yenjoy,
            command=self._update_options,
        )
        yenjoy_cb.pack(side=tk.LEFT, padx=5)

        # --- 詳細オプションフレーム --- (既存のコードを想定)
        details_frame = ttk.Frame(options_frame)
        details_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Label(details_frame, text="データ更新ステップ:").pack(side=tk.LEFT, padx=5)

        options_detail_frame = ttk.Frame(options_frame)
        options_detail_frame.pack(fill=tk.X, padx=5, pady=5)

        # チェックボックスの辞書 (UIBuilder ではなくこちらで管理するべきか？)
        # UIBuilder に step_vars がある前提のコードが _get_selected_steps にあるため、
        # UIBuilder 側で生成・管理する方が一貫性があるかもしれない。
        # ここでは KeirinUpdaterGUI がステップ変数を直接持っていると仮定して進める。
        self.step_checkbuttons = {}  # または UIBuilder の step_vars を使う
        self.step_vars = {  # UIBuilder.step_vars の代わり
            "step1": self.fetch_cups,
            "step2": self.fetch_cup_details,
            "step3": self.fetch_race_data,
            "step4": self.fetch_odds_data,
            "step5": self.fetch_yenjoy_results,
        }
        step_texts = [
            "開催情報取得",
            "開催詳細取得",
            "レース情報取得",
            "オッズデータ取得",
            "Yenjoy結果取得",
        ]

        # 各ステップのチェックボックスを作成
        for i, (step_key, step_var) in enumerate(self.step_vars.items()):
            cb = ttk.Checkbutton(
                options_detail_frame,
                text=step_texts[i],
                variable=step_var,
                command=self._update_options,
            )
            cb.grid(row=i // 2, column=i % 2, sticky="w", padx=5)  # grid で配置する例
            self.step_checkbuttons[step_key] = cb  # ボタン自体も保持

        # --- ★★★ 強制更新オプションを追加 ★★★ ---
        force_update_frame = ttk.Frame(options_frame)
        force_update_frame.pack(
            side=tk.TOP, fill=tk.X, pady=(5, 0), padx=5
        )  # 他の要素の後に追加

        # self.force_update_var は _init_variables で初期化済み
        force_update_check = ttk.Checkbutton(
            force_update_frame,
            text="ステータスを無視して強制更新 (Force update ignoring status)",
            variable=self.force_update_var,
            # command は特に不要
        )
        force_update_check.pack(side=tk.LEFT, padx=5)
        # --- ★★★ 追加ここまで ★★★ ---

        # UIの初期状態を設定 (既存のコードを想定)
        self._update_options()

        return options_frame

    def _update_options(self):
        """更新オプションの変更処理"""
        # ソースの選択状態に応じて、ステップの有効/無効を切り替える
        winticket_enabled = self.update_winticket.get()
        yenjoy_enabled = self.update_yenjoy.get()

        # チェックボックスの状態を更新
        if hasattr(self, "step_checkbuttons"):
            # Winticket関連のチェックボックス
            for step_name in [
                "開催情報取得",
                "開催詳細取得",
                "レース情報取得",
                "オッズデータ取得",
            ]:
                if step_name in self.step_checkbuttons:
                    self.step_checkbuttons[step_name].configure(
                        state="normal" if winticket_enabled else "disabled"
                    )

            # Yenjoy関連のチェックボックス
            if "Yenjoy結果取得" in self.step_checkbuttons:
                self.step_checkbuttons["Yenjoy結果取得"].configure(
                    state="normal" if yenjoy_enabled else "disabled"
                )

        # Winticketが無効なら、関連ステップを無効化
        if not winticket_enabled:
            self.fetch_cups.set(False)
            self.fetch_cup_details.set(False)
            self.fetch_race_data.set(False)
            self.fetch_odds_data.set(False)

        # Yenjoyが無効なら、関連ステップを無効化
        if not yenjoy_enabled:
            self.fetch_yenjoy_results.set(False)

        # 更新マネージャーにオプションを設定
        if hasattr(self, "update_manager"):
            self.update_manager.update_options(
                fetch_cups=self.fetch_cups.get(),
                fetch_cup_details=self.fetch_cup_details.get(),
                fetch_race_data=self.fetch_race_data.get(),
                fetch_odds_data=self.fetch_odds_data.get(),
                fetch_yenjoy_results=self.fetch_yenjoy_results.get(),
            )

        self.logger.debug(
            f"更新オプション変更: Winticket={winticket_enabled}, Yenjoy={yenjoy_enabled}, "
            f"開催={self.fetch_cups.get()}, 詳細={self.fetch_cup_details.get()}, "
            f"レース={self.fetch_race_data.get()}, オッズ={self.fetch_odds_data.get()}, "
            f"Yenjoy結果={self.fetch_yenjoy_results.get()}"
        )

    def _initialize_update_manager(self):
        """更新マネージャーの初期化"""
        if hasattr(self, "update_manager") and self.update_manager:
            # 既存の更新マネージャーのリソースを解放
            try:
                self.update_manager.stop_update()
            except Exception as e:
                self.logger.error(f"既存の更新マネージャーの停止中にエラー: {str(e)}")

        # 更新中フラグ
        self.is_updating = False

        # データベースの初期化
        db_path = self.db_path.get()
        if not db_path:
            self.logger.warning("データベースパスが設定されていません")
            return

        try:
            # MySQL用のKeirinDataAccessorを使用
            # 古いKeirinDatabaseは使用しない（SQLiteエラーの原因）
            if self.db_accessor:
                # 更新マネージャーの作成（db_accessorを使用）
                from gui.update_manager import UpdateManager

                self.update_manager = UpdateManager(
                    self.db_accessor, self.logger, self, self.config
                )  # 引数を修正: db_accessor, logger, controller, config_manager
            else:
                self.logger.error("db_accessorが初期化されていません")
                return

            # 設定の反映
            self._update_options()

            self.logger.info("更新マネージャーを初期化しました")
        except Exception as e:
            self.logger.error(
                f"更新マネージャーの初期化中にエラー: {str(e)}", exc_info=True
            )

    def _initialize_database(self):
        """データベースの初期化処理 (MySQL環境では通常不要)"""
        self.logger.info("MySQL環境ではGUIからのデータベース初期化は通常行いません。")
        messagebox.showinfo(
            "情報",
            "MySQLデータベースの初期化は、サーバー側で管理ツール等を使用して行ってください。",
            parent=self.root,
        )
        # # 確認ダイアログを表示
        # if not messagebox.askyesno(
        #     "確認",
        #     "データベースを初期化します。既存のデータはすべて削除されます。続行しますか？",
        # ):
        #     return
        # db_path = self.db_path.get()
        # if not db_path:
        #     messagebox.showerror("エラー", "データベースパスが設定されていません")
        #     return
        # try:
        #     # UIの更新
        #     self.status_var.set("データベース初期化中...")
        #     self.ui_builder.update_progress(True, "データベース初期化中...")
        #     # データベースの初期化
        #     if self.db_initializer and self.db_initializer.validate_db_path(db_path):
        #         # 既存のデータベースを削除
        #         import os
        #         if os.path.exists(db_path):
        #             try:
        #                 # バックアップを作成
        #                 backup_path = f"{db_path}.bak.{int(datetime.now().timestamp())}"
        #                 import shutil
        #                 shutil.copy2(db_path, backup_path)
        #                 self.logger.info(
        #                     f"既存のDBファイルをバックアップしました: {backup_path}"
        #                 )
        #                 # 既存ファイルを削除
        #                 os.remove(db_path)
        #                 self.logger.info(f"既存のDBファイルを削除しました: {db_path}")
        #             except Exception as e:
        #                 self.logger.error(
        #                     f"データベースファイル操作中にエラー: {str(e)}"
        #                 )
        #                 messagebox.showerror(
        #                     "エラー", f"データベースファイル操作中にエラー: {str(e)}"
        #                 )
        #                 return
        #         # データベースの再作成
        #         self.db_initializer.check_database(db_path, force_initialize=True)
        #         self.logger.info("データベースの初期化が完了しました")
        #         # 完了メッセージ
        #         messagebox.showinfo("完了", "データベースの初期化が完了しました")
        #         # UIの更新
        #         self.status_var.set("準備完了")
        #         self.ui_builder.update_progress(False, "準備完了")
        #     else:
        #         self.logger.error("データベースパスの検証に失敗しました")
        #         messagebox.showerror("エラー", "データベースパスの検証に失敗しました")
        # except Exception as e:
        #     self.logger.error(f"データベース初期化中にエラー: {str(e)}")
        #     messagebox.showerror("エラー", f"データベース初期化中にエラー: {str(e)}")
        #     self.status_var.set("エラー")
        #     self.ui_builder.update_progress(False, "エラー")

    def _show_manual_update_dialog(self):
        """手動更新ダイアログを表示"""
        try:
            # トップレベルウィンドウを作成
            dialog = tk.Toplevel(self.root)
            dialog.title("手動更新")
            dialog.geometry("500x420")
            dialog.transient(self.root)  # メインウィンドウの子ウィンドウとして設定
            dialog.grab_set()  # モーダルダイアログに設定

            # 説明ラベル
            ttk.Label(
                dialog,
                text="レースIDを指定して手動更新を行います。\n形式：cup_id_schedule_index_race_number",
                wraplength=450,
            ).pack(padx=10, pady=10)

            # レースID入力フレーム
            id_frame = ttk.LabelFrame(dialog, text="レースID")
            id_frame.pack(fill=tk.X, padx=10, pady=5)

            race_id_var = tk.StringVar()
            ttk.Entry(id_frame, textvariable=race_id_var, width=40).pack(
                padx=10, pady=10
            )

            # データ選択フレーム
            data_frame = ttk.LabelFrame(dialog, text="取得データ")
            data_frame.pack(fill=tk.X, padx=10, pady=5)

            # 取得データチェックボックス
            race_info_var = tk.BooleanVar(value=True)
            ttk.Checkbutton(data_frame, text="レース情報", variable=race_info_var).pack(
                anchor=tk.W, padx=10, pady=5
            )

            entry_data_var = tk.BooleanVar(value=True)
            ttk.Checkbutton(
                data_frame, text="出走表データ", variable=entry_data_var
            ).pack(anchor=tk.W, padx=10, pady=5)

            odds_data_var = tk.BooleanVar(value=True)
            ttk.Checkbutton(
                data_frame, text="オッズデータ", variable=odds_data_var
            ).pack(anchor=tk.W, padx=10, pady=5)

            result_data_var = tk.BooleanVar(value=True)
            ttk.Checkbutton(
                data_frame, text="結果データ", variable=result_data_var
            ).pack(anchor=tk.W, padx=10, pady=5)

            yenjoy_data_var = tk.BooleanVar(value=True)
            ttk.Checkbutton(
                data_frame, text="Yenjoyラップデータ", variable=yenjoy_data_var
            ).pack(anchor=tk.W, padx=10, pady=5)

            # 実行ステータス表示
            status_frame = ttk.Frame(dialog)
            status_frame.pack(fill=tk.X, padx=10, pady=5)

            status_var = tk.StringVar(value="準備完了")
            ttk.Label(status_frame, textvariable=status_var).pack(side=tk.LEFT, padx=5)

            progress = ttk.Progressbar(status_frame, mode="indeterminate", length=300)
            progress.pack(side=tk.RIGHT, padx=5, fill=tk.X, expand=True)

            # 操作ボタンフレーム
            button_frame = ttk.Frame(dialog)
            button_frame.pack(fill=tk.X, padx=10, pady=10)

            # 実行関数
            def execute_manual_update():
                race_id = race_id_var.get().strip()
                if not race_id:
                    messagebox.showerror("エラー", "レースIDを入力してください")
                    return

                # レースIDの形式チェック
                parts = race_id.split("_")
                if len(parts) < 3:
                    messagebox.showerror(
                        "エラー",
                        "無効なレースID形式です。cup_id_schedule_index_race_number の形式で入力してください",
                    )
                    return

                # 処理中に変更
                status_var.set("更新中...")
                progress.start()
                execute_button.config(state=tk.DISABLED)

                # 別スレッドで実行するための関数
                def run_update():
                    try:
                        # レースIDから cup_id, schedule_index, race_number を抽出
                        schedule_index = int(parts[-2])
                        race_number = int(parts[-1])
                        cup_id = "_".join(parts[:-2])

                        self.logger.info(
                            f"手動更新開始: race_id={race_id}, cup_id={cup_id}, schedule_index={schedule_index}, race_number={race_number}"
                        )

                        # UpdateServiceの初期化
                        if (
                            not hasattr(self, "update_service")
                            or self.update_service is None
                        ):
                            from services.update_service import (
                                UpdateService,
                            )  # UpdateServiceをインポート

                            # UpdateServiceの初期化にはdb_accessorが必要
                            if self.db_accessor:
                                self.update_service = UpdateService(
                                    logger=self.logger,
                                    db_accessor=self.db_accessor,
                                    config_manager=self.config,  # db_pathではなくdb_accessorを渡す。config_managerも渡す
                                )
                            else:
                                self.logger.error(
                                    "手動更新ダイアログ: db_accessorがありません。UpdateServiceを初期化できません。"
                                )
                                dialog.after(
                                    0, lambda: status_var.set("エラー: DB接続情報なし")
                                )
                                dialog.after(0, lambda: progress.stop())
                                dialog.after(
                                    0, lambda: execute_button.config(state=tk.NORMAL)
                                )
                                return

                        success = True
                        message = "更新完了"

                        # 各データの更新
                        if race_info_var.get():
                            success, message = self.update_service.update_race_info(
                                cup_id, schedule_index, race_number
                            )
                            if not success:
                                dialog.after(
                                    0, lambda: status_var.set(f"エラー: {message}")
                                )
                                dialog.after(0, lambda: progress.stop())
                                dialog.after(
                                    0, lambda: execute_button.config(state=tk.NORMAL)
                                )
                                return

                        if entry_data_var.get():
                            success, message = self.update_service.update_entry_data(
                                cup_id, schedule_index, race_number
                            )
                            if not success:
                                dialog.after(
                                    0, lambda: status_var.set(f"エラー: {message}")
                                )
                                dialog.after(0, lambda: progress.stop())
                                dialog.after(
                                    0, lambda: execute_button.config(state=tk.NORMAL)
                                )
                                return

                        if odds_data_var.get():
                            success, message = self.update_service.update_odds_data(
                                cup_id, schedule_index, race_number
                            )
                            if not success:
                                dialog.after(
                                    0, lambda: status_var.set(f"エラー: {message}")
                                )
                                dialog.after(0, lambda: progress.stop())
                                dialog.after(
                                    0, lambda: execute_button.config(state=tk.NORMAL)
                                )
                                return

                        if result_data_var.get():
                            success, message = self.update_service.update_result_data(
                                cup_id, schedule_index, race_number
                            )
                            if not success:
                                dialog.after(
                                    0, lambda: status_var.set(f"エラー: {message}")
                                )
                                dialog.after(0, lambda: progress.stop())
                                dialog.after(
                                    0, lambda: execute_button.config(state=tk.NORMAL)
                                )
                                return

                        if yenjoy_data_var.get():
                            success, message = self.update_service.update_yenjoy_result(
                                race_id
                            )
                            if not success:
                                dialog.after(
                                    0, lambda: status_var.set(f"エラー: {message}")
                                )
                                dialog.after(0, lambda: progress.stop())
                                dialog.after(
                                    0, lambda: execute_button.config(state=tk.NORMAL)
                                )
                                return

                        # 処理完了
                        dialog.after(0, lambda: status_var.set("更新完了"))
                        dialog.after(0, lambda: progress.stop())
                        dialog.after(0, lambda: execute_button.config(state=tk.NORMAL))
                        dialog.after(
                            0,
                            lambda: messagebox.showinfo(
                                "完了", "手動更新が完了しました"
                            ),
                        )

                    except Exception as e:
                        self.logger.error(
                            f"手動更新中にエラーが発生: {str(e)}", exc_info=True
                        )
                        dialog.after(
                            0,
                            lambda err_msg=str(e): status_var.set(f"エラー: {err_msg}"),
                        )
                        dialog.after(0, lambda: progress.stop())
                        dialog.after(0, lambda: execute_button.config(state=tk.NORMAL))
                        dialog.after(
                            0,
                            lambda err_msg=str(e): messagebox.showerror(
                                "エラー", f"更新処理でエラーが発生しました: {err_msg}"
                            ),
                        )

                # 別スレッドで実行
                import threading

                update_thread = threading.Thread(target=run_update)
                update_thread.daemon = True
                update_thread.start()

            # 実行ボタン
            execute_button = ttk.Button(
                button_frame, text="実行", command=execute_manual_update, width=10
            )
            execute_button.pack(side=tk.LEFT, padx=5)

            # キャンセルボタン
            ttk.Button(
                button_frame, text="閉じる", command=dialog.destroy, width=10
            ).pack(side=tk.RIGHT, padx=5)

            # ダイアログを中央に配置
            dialog.update_idletasks()
            w = dialog.winfo_width()
            h = dialog.winfo_height()
            x = (self.root.winfo_width() - w) // 2 + self.root.winfo_x()
            y = (self.root.winfo_height() - h) // 2 + self.root.winfo_y()
            dialog.geometry(f"{w}x{h}+{x}+{y}")

        except Exception as e:
            self.logger.error(
                f"手動更新ダイアログの表示中にエラーが発生: {str(e)}", exc_info=True
            )
            messagebox.showerror(
                "エラー", f"ダイアログ表示中にエラーが発生しました: {str(e)}"
            )

    def _init_logging(self):
        """ロギングの初期化"""
        # ログマネージャーの初期化
        from gui.log_manager import LogManager

        self.log_manager = LogManager(self)
        self.logger = self.log_manager.logger
        self.logger.info("ロギングを初期化しました")

    def open_config_window(self):
        """コンフィグ編集用の新しいウィンドウを開く"""
        self.logger.info("コンフィグ画面を開きます")

        config_win = tk.Toplevel(self.root)
        config_win.title("設定編集")
        # ウィンドウサイズを調整 (スケジュール表示のため広げる)
        config_win.geometry("600x550")
        config_win.transient(self.root)  # 親ウィンドウの上に表示
        config_win.grab_set()  # モーダルウィンドウにする

        # --- UI要素の作成 ---
        main_frame = ttk.Frame(config_win, padding="10")
        main_frame.pack(expand=True, fill=tk.BOTH)

        # --- システム設定セクション ---
        system_frame = ttk.LabelFrame(main_frame, text="システム設定", padding="10")
        system_frame.pack(fill=tk.X, pady=5)

        ttk.Label(system_frame, text="ログレベル:").grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=2
        )
        self.cfg_log_level = tk.StringVar()  # 変数定義
        log_level_combo = ttk.Combobox(
            system_frame,
            textvariable=self.cfg_log_level,
            values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            width=10,
        )
        log_level_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=2)

        # --- API設定セクション ---
        api_frame = ttk.LabelFrame(main_frame, text="API設定", padding="10")
        api_frame.pack(fill=tk.X, pady=5)

        ttk.Label(api_frame, text="リクエストタイムアウト(秒):").grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=2
        )
        self.cfg_request_timeout = tk.IntVar()  # 変数定義
        ttk.Entry(api_frame, textvariable=self.cfg_request_timeout, width=5).grid(
            row=0, column=1, sticky=tk.W, padx=5, pady=2
        )

        ttk.Label(api_frame, text="リトライ回数:").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=2
        )
        self.cfg_retry_count = tk.IntVar()  # 変数定義
        ttk.Entry(api_frame, textvariable=self.cfg_retry_count, width=5).grid(
            row=1, column=1, sticky=tk.W, padx=5, pady=2
        )

        ttk.Label(api_frame, text="リトライ遅延(秒):").grid(
            row=2, column=0, sticky=tk.W, padx=5, pady=2
        )
        self.cfg_retry_delay = tk.IntVar()  # 変数定義
        ttk.Entry(api_frame, textvariable=self.cfg_retry_delay, width=5).grid(
            row=2, column=1, sticky=tk.W, padx=5, pady=2
        )

        # --- スケジュール設定セクション ---
        schedule_frame = ttk.LabelFrame(
            main_frame, text="スケジュール設定", padding="10"
        )
        schedule_frame.pack(expand=True, fill=tk.BOTH, pady=5)

        # スケジュール一覧表示 (Treeview)
        schedule_cols = ("time", "steps", "enabled")
        self.schedule_tree = ttk.Treeview(
            schedule_frame, columns=schedule_cols, show="headings", height=5
        )
        # --- Treeview のヘッダーとカラム設定を追加 ---
        self.schedule_tree.heading("time", text="実行時刻 (HH:MM)")
        self.schedule_tree.heading("steps", text="実行ステップ")
        self.schedule_tree.heading("enabled", text="有効")
        self.schedule_tree.column("time", width=100, anchor=tk.CENTER, stretch=tk.NO)
        self.schedule_tree.column("steps", width=250)
        self.schedule_tree.column("enabled", width=60, anchor=tk.CENTER, stretch=tk.NO)
        # ---------------------------------------------
        # ... (Treeviewの設定) ...
        self.schedule_tree.grid(row=0, column=0, sticky="nsew")
        # --- Scrollbar の設定と配置を追加 ---
        scrollbar = ttk.Scrollbar(
            schedule_frame, orient=tk.VERTICAL, command=self.schedule_tree.yview
        )
        self.schedule_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=0, column=1, sticky="ns")
        schedule_frame.rowconfigure(0, weight=1)
        schedule_frame.columnconfigure(0, weight=1)
        # -------------------------------------
        # --- ボタンのフレームと配置を追加 ---
        schedule_btn_frame = ttk.Frame(schedule_frame)
        schedule_btn_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(5, 0))

        add_button = ttk.Button(
            schedule_btn_frame, text="追加", command=self._add_schedule_entry
        )
        add_button.pack(side=tk.LEFT, padx=5)
        edit_button = ttk.Button(
            schedule_btn_frame, text="編集", command=self._edit_schedule_entry
        )
        edit_button.pack(side=tk.LEFT, padx=5)
        delete_button = ttk.Button(
            schedule_btn_frame, text="削除", command=self._delete_schedule_entry
        )
        delete_button.pack(side=tk.LEFT, padx=5)
        # -----------------------------------
        # ... (ボタンの設定) ...

        # --- Deploymentセクション ---
        deploy_frame = ttk.LabelFrame(main_frame, text="デプロイ設定", padding="10")
        deploy_frame.pack(fill=tk.X, pady=5)

        self.auto_deploy_var = tk.BooleanVar()  # 変数定義
        auto_deploy_check = ttk.Checkbutton(
            deploy_frame,
            text="更新後に自動でDuckDBにデプロイする",
            variable=self.auto_deploy_var,
        )
        auto_deploy_check.grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=2)

        ttk.Label(deploy_frame, text="DuckDBパス:").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=2
        )
        self.frontend_db_path_var = tk.StringVar()  # 変数定義
        db_path_entry = ttk.Entry(
            deploy_frame, textvariable=self.frontend_db_path_var, width=40
        )
        db_path_entry.grid(row=1, column=1, sticky=tk.EW, padx=5, pady=2)
        browse_button = ttk.Button(
            deploy_frame,
            text="参照...",
            command=lambda: self._browse_duckdb_path(self.frontend_db_path_var),
        )
        browse_button.grid(row=1, column=2, padx=5, pady=2)
        deploy_frame.columnconfigure(1, weight=1)

        # --- ボタンフレーム ---
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        save_button = ttk.Button(
            button_frame,
            text="保存して閉じる",
            command=lambda: self.save_config_from_window(config_win),
        )
        save_button.pack(side=tk.RIGHT, padx=5)
        cancel_button = ttk.Button(
            button_frame, text="キャンセル", command=config_win.destroy
        )
        cancel_button.pack(side=tk.RIGHT, padx=5)

        # --- 初期値の読み込み (UI要素作成後に行う) ---
        try:
            # config オブジェクトから値を取得
            # システム設定
            log_level_val = self.config.get_value(
                "System", "log_level", fallback="INFO"
            )
            self.cfg_log_level.set(log_level_val)
            # API設定
            self.cfg_request_timeout.set(
                self.config.get_int("API", "request_timeout", fallback=30)
            )
            self.cfg_retry_count.set(
                self.config.get_int("API", "retry_count", fallback=3)
            )
            self.cfg_retry_delay.set(
                self.config.get_int("API", "retry_delay", fallback=5)
            )
            # デプロイ設定
            auto_deploy = self.config.get_boolean(
                "Deployment", "auto_deploy_after_update", fallback=False
            )
            frontend_db_path = self.config.get_value(
                "Deployment", "frontend_db_path", fallback="frontend.duckdb"
            )
            self.auto_deploy_var.set(auto_deploy)
            self.frontend_db_path_var.set(frontend_db_path)

            # スケジュール読み込み
            self._load_schedule_to_treeview()

        except Exception as e:
            self.logger.error(f"設定値の読み込み中にエラー: {e}")
            messagebox.showerror(
                "エラー", f"設定の読み込みに失敗しました:\n{e}", parent=config_win
            )
            # エラー時は安全なデフォルト値を設定
            self.cfg_log_level.set("INFO")
            self.cfg_request_timeout.set(30)
            self.cfg_retry_count.set(3)
            self.cfg_retry_delay.set(5)
            self.auto_deploy_var.set(False)
            self.frontend_db_path_var.set("frontend.duckdb")
            # スケジュールは空にする
            self._load_schedule_to_treeview()  # 空リストが読み込まれるはず

        # スケジューラー設定 (セクション 'Scheduler', オプション名と仮定)
        self.scheduler_enabled = tk.BooleanVar(
            value=self.config.get_boolean("Scheduler", "enabled", fallback=False)
        )
        self.schedule_time = tk.StringVar(
            value=self.config.get_value("Scheduler", "time", fallback="03:00")
        )

        # DuckDB出力パス (セクション 'Deployment', オプション 'frontend_db_path')
        default_duckdb_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "frontend.duckdb",
        )
        duckdb_path_value = self.config.get_value(
            "Deployment", "frontend_db_path", fallback=default_duckdb_path
        )
        self.duckdb_path = tk.StringVar(value=duckdb_path_value)

    def _load_schedule_to_treeview(self):
        """ConfigManagerからスケジュールを読み込みTreeviewに表示"""
        # 既存の項目をクリア
        if hasattr(self, "schedule_tree"):  # Treeviewが存在するか確認
            for item in self.schedule_tree.get_children():
                self.schedule_tree.delete(item)
        else:
            self.logger.error(
                "_load_schedule_to_treeview: schedule_tree が見つかりません。"
            )
            return

        # Configから読み込み
        schedule_list = self.config.get_schedule_list(fallback=[])

        # Treeviewにデータを格納する際、内部的にリストも保持すると管理しやすい
        self._current_schedule_list_in_dialog = list(
            schedule_list
        )  # ダイアログ編集中の一時リスト

        for i, item in enumerate(self._current_schedule_list_in_dialog):
            time_str = item.get("time", "??:??")
            steps = item.get("steps", [])
            steps_str = (
                "all"
                if steps == "all"
                else (",".join(steps) if isinstance(steps, list) else str(steps))
            )
            enabled_str = "有効" if item.get("enabled", False) else "無効"
            # iid としてリストのインデックスを使う
            try:
                self.schedule_tree.insert(
                    "", tk.END, iid=str(i), values=(time_str, steps_str, enabled_str)
                )
            except tk.TclError as e:
                self.logger.error(
                    f"Treeviewへの挿入中にエラー (iid重複?): {e}, item={item}"
                )
                # 重複iidエラーの場合の対処など

    def _browse_duckdb_path(self, path_var):
        """DuckDBファイルの保存場所を選択するダイアログ"""
        file_path = filedialog.asksaveasfilename(
            title="DuckDBファイルの保存先を選択",
            initialdir=os.path.dirname(path_var.get()),
            initialfile=os.path.basename(path_var.get()),
            filetypes=[("DuckDB", "*.duckdb"), ("すべてのファイル", "*.*")],
            defaultextension=".duckdb",
        )
        if file_path:
            path_var.set(file_path)

    def save_config_from_window(self, window):
        """コンフィグ編集ウィンドウから設定を保存する"""
        try:
            # デプロイ設定 (既存)
            auto_deploy = self.auto_deploy_var.get()
            frontend_db_path = self.frontend_db_path_var.get()

            # config オブジェクトに値を設定 (set_value)
            # システム設定
            self.config.set_value("System", "log_level", self.cfg_log_level.get())
            # API設定 (整数値も文字列で保存されるが ConfigManager が get_int で読む)
            self.config.set_value(
                "API", "request_timeout", self.cfg_request_timeout.get()
            )
            self.config.set_value("API", "retry_count", self.cfg_retry_count.get())
            self.config.set_value("API", "retry_delay", self.cfg_retry_delay.get())

            # --- スケジュール設定 (Treeviewからデータを取得して保存) ---
            # ダイアログ編集中の一時リスト (_current_schedule_list_in_dialog) を使う
            if hasattr(self, "_current_schedule_list_in_dialog"):
                self.config.set_schedule_list(self._current_schedule_list_in_dialog)
            else:
                self.logger.warning(
                    "保存するスケジュール情報が見つかりません。スキップします。"
                )

            # --- 自動更新設定 (旧) は削除 ---
            # self.config.set_value('AutoUpdate', 'enabled', self.cfg_auto_update_enabled.get())
            # self.config.set_value('AutoUpdate', 'interval_minutes', self.cfg_auto_update_interval.get())

            # デプロイ設定 (既存)
            self.config.set_value("Deployment", "auto_deploy_after_update", auto_deploy)
            self.config.set_value("Deployment", "frontend_db_path", frontend_db_path)

            # 設定をファイルに保存 (save_config)
            save_success = self.config.save_config()

            if save_success:
                # 保存成功時のメッセージは変更なし
                self.logger.info("設定を保存しました")
                messagebox.showinfo(
                    "保存完了",
                    "設定を保存しました。スケジュール変更の反映にはアプリの再起動が必要な場合があります。",
                    parent=window,
                )
                window.destroy()
                # スケジューラーを再起動/リロードする処理を呼び出す
                self._restart_scheduler()
            else:
                self.logger.error("設定ファイルの保存に失敗しました。")
                messagebox.showerror(
                    "エラー",
                    "設定ファイルの保存に失敗しました。ログを確認してください。",
                    parent=window,
                )

        except Exception as e:
            self.logger.error(f"設定の保存中にエラー: {e}")
            messagebox.showerror(
                "エラー", f"設定の保存に失敗しました:\n{e}", parent=window
            )

    def run_deploy_to_duckdb(self):
        """「DuckDB更新」ボタンが押されたときの処理 (SQLite -> DuckDBへの全データデプロイ)"""
        if self.deploy_running:  # デプロイ中なら実行しない
            messagebox.showwarning(
                "処理中", "現在、別の更新またはデプロイ処理が実行中です。"
            )
            return

        # ★ 手動更新開始前にスケジューラーを停止 ★
        self._stop_scheduler()
        self.logger.info("手動デプロイ開始のためスケジューラーを停止しました。")

        frontend_db_path = self.config.get_value(
            "Deployment", "frontend_db_path", fallback="frontend.duckdb"
        )
        if not messagebox.askyesno(
            "確認",
            f"現在のSQLiteデータベースの内容を全て以下のDuckDBファイルにデプロイします。\n\n{frontend_db_path}\n\nよろしいですか？",
        ):
            self._start_scheduler()  # キャンセルされたらスケジューラーを再開
            return

        # self.update_running = True # 更新はしない
        self.deploy_running = True  # デプロイも同時に行うフラグとして
        # self.manual_update_running = True # フラグは不要
        self._disable_relevant_buttons(deploy=True)  # deploy=True でボタンを無効化
        self.update_progress_label.config(text="DuckDBへのデプロイ準備中...")
        self.progress_bar.start(10)

        # デプロイのみを実行するスレッド
        thread = threading.Thread(target=self._deploy_thread, daemon=True)
        thread.start()

    def _deploy_thread(self):
        """SQLiteからDuckDBへの全テーブルデプロイをバックグラウンドで実行するスレッド"""
        deploy_success = False
        try:
            # ★ self.root.after を使うように修正 ★
            self.root.after(
                0,
                lambda: self.update_progress_label.config(text="DuckDBへデプロイ中..."),
            )
            # ★ self._logger -> self.logger に修正 ★
            self.logger.info("デプロイ処理を開始します。")
            try:
                # DeploymentService と FrontendDatabase をインポート・初期化
                from database.frontend_database import FrontendDatabase
                from services.deployment_service import DeploymentService

                frontend_db_path = self.config.get_value(
                    "Deployment", "frontend_db_path", fallback="frontend.duckdb"
                )
                frontend_db = FrontendDatabase(
                    frontend_db_path, logger=self.logger
                )  # ★ self.logger を渡す
                # KeirinDatabase インスタンスを渡す (self.db)
                deployment_service = DeploymentService(
                    self.db_accessor, frontend_db, self.logger
                )  # ★ self.logger を渡す

                deploy_success = deployment_service.deploy_all_tables()
                if deploy_success:
                    # ★ self._logger -> self.logger ★
                    self.logger.info(
                        f"DuckDBへのデプロイが正常に完了しました: {frontend_db_path}"
                    )
                else:
                    # ★ self._logger -> self.logger ★
                    self.logger.error(
                        "DuckDBへのデプロイ処理中にエラーが発生しました。"
                    )

            except ImportError as ie:
                # ★ self._logger -> self.logger ★
                self.logger.error(f"デプロイに必要なモジュールのインポートに失敗: {ie}")
                # GUI要素はメインスレッドから操作
                # ★ self.master -> self.root ★
                self.root.after(
                    0,
                    lambda err_module=str(ie): messagebox.showerror(
                        "エラー",
                        f"デプロイに必要なモジュールが見つかりません:\n{err_module}",
                    ),
                )
            except Exception as e:
                # ★ self._logger -> self.logger ★
                self.logger.error(
                    f"デプロイ処理中に予期せぬエラーが発生しました: {e}", exc_info=True
                )
                # GUI要素はメインスレッドから操作
                # ★ self.master -> self.root ★
                self.root.after(
                    0,
                    lambda err_deploy=str(e): messagebox.showerror(
                        "デプロイエラー",
                        f"デプロイ中にエラーが発生しました:\n{err_deploy}",
                    ),
                )

            # --- 結果表示 ---
            final_message = ""
            message_type = "info"
            if deploy_success:
                final_message = "DuckDBへのデプロイが正常に完了しました。"
                message_type = "info"
            else:
                final_message = "DuckDBへのデプロイに失敗しました。"
                message_type = "error"

            # ★ self._logger -> self.logger ★
            self.logger.info(final_message)
            # ★ self.master -> self.root ★
            self.root.after(
                0,
                lambda: self.update_progress_label.config(
                    text="デプロイ完了" if deploy_success else "デプロイ失敗"
                ),
            )
            # メッセージボックス表示はメインスレッドから
            # ★ self.master -> self.root ★
            self.root.after(
                100,
                lambda: (
                    messagebox.showinfo("完了", final_message)
                    if message_type == "info"
                    else (
                        messagebox.showwarning("完了（一部失敗）", final_message)
                        if message_type == "warning"
                        else messagebox.showerror("エラー", final_message)
                    )
                ),
            )

        except Exception as e:
            # ★ self._logger -> self.logger ★
            self.logger.error(
                f"デプロイ処理中に予期せぬエラーが発生しました: {e}", exc_info=True
            )
            # ★ self.master -> self.root ★
            self.root.after(
                0, lambda: self.update_progress_label.config(text="デプロイエラー")
            )
            # ★ self.master -> self.root ★
            self.root.after(
                100,
                lambda err_final=str(e): messagebox.showerror(
                    "エラー", f"処理中に予期せぬエラーが発生しました:\n{err_final}"
                ),
            )
        finally:
            # self.update_running = False
            self.deploy_running = False
            # self.manual_update_running = False # フラグは不要
            # ★ self.master -> self.root ★
            self.root.after(0, lambda: self.progress_bar.stop())
            # ★ self.master -> self.root ★
            self.root.after(0, self._enable_relevant_buttons)  # ボタン状態を更新
            # ★ 手動更新完了後にスケジューラーを再開 ★
            # ★ self.master -> self.root ★
            self.root.after(100, self._start_scheduler)  # 少し待ってから再開
            # ★ self._logger -> self.logger ★
            self.logger.info("手動デプロイ完了のためスケジューラーを再開しました。")

    def _check_schedule(self):
        """スケジュールをチェックし、実行時刻なら更新を開始する"""
        # フラグチェックは不要

        # スケジューラ自体が停止していればチェックしない (タイマーが動かないはず)
        if not hasattr(self, "_scheduler_timer") or self._scheduler_timer is None:
            self.logger.debug(
                "スケジューラー停止中のため、スケジュールチェックをスキップします。"
            )
            return

        # now = datetime.now() # _perform_schedule_check に移動
        # current_time_str = now.strftime("%H:%M") # _perform_schedule_check に移動
        # ... (以降の処理は変更なし) ...
        # _perform_schedule_check を呼び出す
        self._perform_schedule_check()

    def _add_schedule_entry(self):
        """スケジュール追加ダイアログを開く"""
        # 新しい空のスケジュールデータ
        new_schedule = {"time": "00:00", "steps": [], "enabled": True}
        result = self._open_schedule_edit_dialog(new_schedule, is_new=True)
        if result:  # 保存されたら
            # Treeviewと内部リストに追加
            self._current_schedule_list_in_dialog.append(result)
            self._update_treeview_from_list(self._current_schedule_list_in_dialog)

    def _edit_schedule_entry(self):
        """選択されたスケジュールを編集するダイアログを開く"""
        selected_items = self.schedule_tree.selection()
        if not selected_items:
            messagebox.showwarning(
                "選択エラー",
                "編集するスケジュールを選択してください。",
                parent=self.schedule_tree.winfo_toplevel(),
            )
            return

        item_iid = selected_items[0]  # 最初の選択項目
        try:
            item_index = int(item_iid)
            if not (0 <= item_index < len(self._current_schedule_list_in_dialog)):
                raise ValueError("無効なインデックス")
            original_data = self._current_schedule_list_in_dialog[item_index]
        except (ValueError, IndexError):
            messagebox.showerror(
                "エラー",
                "スケジュールデータの取得に失敗しました。",
                parent=self.schedule_tree.winfo_toplevel(),
            )
            return

        result = self._open_schedule_edit_dialog(
            original_data.copy(), is_new=False
        )  # コピーを渡す
        if result:
            # Treeviewと内部リストを更新
            self._current_schedule_list_in_dialog[item_index] = result
            self._update_treeview_from_list(self._current_schedule_list_in_dialog)

    def _delete_schedule_entry(self):
        """選択されたスケジュールを削除する"""
        selected_items = self.schedule_tree.selection()
        if not selected_items:
            messagebox.showwarning(
                "選択エラー",
                "削除するスケジュールを選択してください。",
                parent=self.schedule_tree.winfo_toplevel(),
            )
            return

        if messagebox.askyesno(
            "確認",
            "選択されたスケジュールを削除しますか？",
            parent=self.schedule_tree.winfo_toplevel(),
        ):
            indices_to_delete = sorted(
                [int(iid) for iid in selected_items], reverse=True
            )

            new_list = []
            for i, item in enumerate(self._current_schedule_list_in_dialog):
                if i not in indices_to_delete:
                    new_list.append(item)

            # Treeviewと内部リスト更新
            self._current_schedule_list_in_dialog = new_list
            self._update_treeview_from_list(self._current_schedule_list_in_dialog)

    def _update_treeview_from_list(self, schedule_list):
        """内部リストからTreeviewを再構築"""
        # 既存の項目をクリア
        for item in self.schedule_tree.get_children():
            self.schedule_tree.delete(item)
        # リストから再挿入
        for i, item in enumerate(schedule_list):
            time_str = item.get("time", "??:??")
            steps = item.get("steps", [])
            steps_str = (
                "all"
                if steps == "all"
                else (",".join(steps) if isinstance(steps, list) else str(steps))
            )
            enabled_str = "有効" if item.get("enabled", False) else "無効"
            self.schedule_tree.insert(
                "", tk.END, iid=str(i), values=(time_str, steps_str, enabled_str)
            )

    def _open_schedule_edit_dialog(self, schedule_data, is_new):
        """スケジュール追加/編集用ダイアログ"""
        dialog = tk.Toplevel(self.root)
        dialog.title("スケジュール編集" if not is_new else "スケジュール追加")
        dialog.geometry("350x350")
        dialog.transient(self.root)
        dialog.grab_set()

        result_data = {}  # ダイアログの結果を格納

        # --- UI ---
        main_frame = ttk.Frame(dialog, padding="10")
        main_frame.pack(expand=True, fill=tk.BOTH)

        # 時間設定
        time_frame = ttk.LabelFrame(main_frame, text="実行時刻")
        time_frame.pack(fill=tk.X, pady=5)
        hour_var = tk.StringVar(value=schedule_data.get("time", "00:00").split(":")[0])
        minute_var = tk.StringVar(
            value=schedule_data.get("time", "00:00").split(":")[1]
        )
        ttk.Spinbox(
            time_frame,
            from_=0,
            to=23,
            textvariable=hour_var,
            wrap=True,
            width=3,
            format="%02.0f",
        ).pack(side=tk.LEFT, padx=5)
        ttk.Label(time_frame, text=":").pack(side=tk.LEFT)
        ttk.Spinbox(
            time_frame,
            from_=0,
            to=59,
            textvariable=minute_var,
            wrap=True,
            width=3,
            format="%02.0f",
        ).pack(side=tk.LEFT, padx=5)

        # ステップ設定
        steps_frame = ttk.LabelFrame(main_frame, text="実行ステップ")
        steps_frame.pack(fill=tk.X, pady=5)
        step_vars = {}  # 各ステップの BooleanVar を格納
        all_steps_var = tk.BooleanVar(value=(schedule_data.get("steps") == "all"))
        # schedule_data["steps"] がリストでない場合（'all' など）は空リストにする
        initial_steps = (
            schedule_data.get("steps", [])
            if isinstance(schedule_data.get("steps"), list)
            else []
        )

        def toggle_all_steps():
            state = tk.DISABLED if all_steps_var.get() else tk.NORMAL
            for step, var_cb in step_vars.items():
                var_cb["cb"].config(state=state)
                if state == tk.DISABLED:
                    var_cb["var"].set(False)  # 全て選択時は個別チェックを外す

        ttk.Checkbutton(
            steps_frame,
            text="全てのステップを実行",
            variable=all_steps_var,
            command=toggle_all_steps,
        ).grid(row=0, column=0, columnspan=2, sticky=tk.W)

        # 利用可能なステップ名リスト (どこかで定義されていると良い)
        # 例: self.controller.available_steps = ['step1', 'step2', 'step3', 'step4', 'step5']
        available_steps = ["step1", "step2", "step3", "step4", "step5"]  # 仮

        for i, step in enumerate(available_steps):
            var = tk.BooleanVar(value=(step in initial_steps))
            cb = ttk.Checkbutton(
                steps_frame,
                text=f"ステップ {i+1} ({step})".replace("step", ""),
                variable=var,
            )
            cb.grid(row=i // 2 + 1, column=i % 2, sticky=tk.W, padx=10)
            step_vars[step] = {"var": var, "cb": cb}  # チェックボックス自体も保持
        toggle_all_steps()  # 初期状態を反映

        # 有効/無効設定
        enabled_frame = ttk.LabelFrame(main_frame, text="有効化")
        enabled_frame.pack(fill=tk.X, pady=5)
        enabled_var = tk.BooleanVar(value=schedule_data.get("enabled", True))
        ttk.Checkbutton(
            enabled_frame, text="このスケジュールを有効にする", variable=enabled_var
        ).pack(anchor=tk.W, padx=10)

        # ボタン
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)

        def on_save():
            # 入力値を取得して result_data に格納
            # 時間結合とゼロ埋め
            try:
                hour_str = f"{int(hour_var.get()):02d}"
                minute_str = f"{int(minute_var.get()):02d}"
                time_str = f"{hour_str}:{minute_str}"
                # 時刻形式チェック
                datetime.strptime(time_str, "%H:%M")
            except (ValueError, TypeError):
                messagebox.showerror(
                    "入力エラー",
                    "時刻の形式が正しくありません (HH:MM)。",
                    parent=dialog,
                )
                return

            steps_list = []
            if all_steps_var.get():
                steps_list = "all"
            else:
                steps_list = [
                    step for step, var_cb in step_vars.items() if var_cb["var"].get()
                ]
                if not steps_list:  # 個別ステップが一つも選択されていない場合
                    messagebox.showwarning(
                        "入力エラー",
                        "実行するステップを1つ以上選択するか、「全てのステップを実行」をチェックしてください。",
                        parent=dialog,
                    )
                    return

            result_data["time"] = time_str
            result_data["steps"] = steps_list
            result_data["enabled"] = enabled_var.get()

            dialog.destroy()  # 保存して閉じる

        ttk.Button(button_frame, text="保存", command=on_save).pack(
            side=tk.RIGHT, padx=5
        )
        ttk.Button(button_frame, text="キャンセル", command=dialog.destroy).pack(
            side=tk.RIGHT, padx=5
        )

        dialog.wait_window()  # ダイアログが閉じるまで待機

        # 保存ボタンが押された場合のみ result_data が空でない
        return result_data if result_data else None

    # --- スケジューリング関連メソッド ---
    def _start_scheduler(self):
        """スケジューラーを開始する"""
        if not hasattr(self, "_scheduler_timer") or self._scheduler_timer is None:
            self._scheduler_timer = None  # タイマーオブジェクトがないことを示すマーカー
            self.logger.info("スケジューラーを開始します。")
            # 最初のタイマーをセット (直接 _schedule_check を呼ばずにタイマー経由で開始)
            import threading

            # 少し待ってから最初のチェックを開始する（起動直後の負荷軽減）
            self._scheduler_timer = threading.Timer(
                1.0, self._schedule_check
            )  # 1秒後に初回実行
            self._scheduler_timer.daemon = True
            self._scheduler_timer.start()
            self.logger.info("最初のスケジュールチェックを1秒後にセットしました。")
        else:
            self.logger.warning("スケジューラーは既に開始されています。")

    def _stop_scheduler(self):
        """スケジューラーを停止する"""
        if hasattr(self, "_scheduler_timer") and self._scheduler_timer:
            self._scheduler_timer.cancel()
            self._scheduler_timer = None  # タイマー停止を示す
            self.logger.info("スケジューラーを停止しました。")

    def _restart_scheduler(self):
        """スケジューラーを再起動する"""
        self.logger.info("スケジューラーを再起動します。設定変更を反映します。")
        self._stop_scheduler()
        # 少し待ってから再開 (停止処理完了を待つ意味合いも込めて)
        self.root.after(100, self._start_scheduler)

    def _schedule_check(self):
        """定期的にスケジュールをチェックするタイマーコールバック"""
        self.logger.info("スケジュールチェックタイマー実行 (_schedule_check)")

        # 実際のチェック処理を実行
        self._perform_schedule_check()

        # 次のチェックをスケジュール
        try:
            # _scheduler_timer が None でない（停止されていない）場合のみ次をスケジュール
            if hasattr(self, "_scheduler_timer") and self._scheduler_timer is not None:
                import threading

                self._scheduler_timer = threading.Timer(
                    60.0, self._schedule_check
                )  # 次回も _schedule_check を呼ぶ
                self._scheduler_timer.daemon = True
                self._scheduler_timer.start()
                self.logger.info("次のスケジュールチェックを60秒後にセットしました。")
            else:
                self.logger.info(
                    "スケジューラーが停止中のため、次のチェックはスケジュールしません。"
                )
        except Exception as e:
            self.logger.error(
                f"次のスケジュールチェックの設定中にエラー: {e}", exc_info=True
            )

    def _perform_schedule_check(self):
        """現在の時刻とスケジュールを比較し、実行すべきタスクがあれば実行する (旧 _check_schedule)"""
        self.logger.info("_perform_schedule_check 関数が呼び出されました。")
        now = datetime.now()
        current_time_str = now.strftime("%H:%M")  # ここで定義

        # last_schedule_check_time_str が未定義の場合のエラーを防ぐため、 hasattr で確認
        if (
            hasattr(self, "_last_schedule_check_time_str")
            and self._last_schedule_check_time_str == current_time_str
        ):
            self.logger.debug(f"時刻 {current_time_str} はチェック済みのためスキップ")
            return

        self.logger.info(f"今回のチェック時刻を記録: {current_time_str}")
        self._last_schedule_check_time_str = (
            current_time_str  # ここで属性を初期化 or 更新
        )

        try:
            schedule_list = self.config.get_schedule_list(fallback=[])
            self.logger.info(f"読み込んだスケジュールリスト: {schedule_list}")
            for schedule in schedule_list:
                if not schedule.get("enabled", False):
                    self.logger.info(f"スケジュール無効のためスキップ: {schedule}")
                    continue

                schedule_time = schedule.get("time")
                is_enabled = schedule.get("enabled", False)
                self.logger.info(
                    f"Checking schedule: Time='{schedule_time}', Enabled={is_enabled}, CurrentTime='{current_time_str}'"
                )

                self.logger.info(
                    f"時刻比較: schedule_time='{schedule_time}', current_time_str='{current_time_str}', is_enabled={is_enabled}"
                )
                if is_enabled and schedule_time == current_time_str:
                    self.logger.info(f"スケジュール実行時刻です: {schedule_time}")
                    if (
                        self.is_updating
                    ):  # self.update_running でも良いが、is_updating の方が状態を示している
                        self.logger.warning(
                            "現在、別の更新処理が実行中のため、スケジュールされた更新をスキップします。"
                        )
                    else:
                        steps = schedule.get("steps", "all")
                        self.logger.info(
                            f"run_scheduled_update を呼び出します。Steps: {steps}"
                        )
                        self.root.after(0, self.run_scheduled_update, steps)
                    # 同じ時刻に複数スケジュールがあっても最初の1つだけ実行する仕様なら break は正しい
                    break
                else:
                    self.logger.info(
                        "時刻不一致または無効なため、このスケジュールは実行しません。"
                    )
        except Exception as e:
            # 関数名を修正
            self.logger.error(
                f"_perform_schedule_check 中にエラーが発生: {e}", exc_info=True
            )

    def run_scheduled_update(self, steps):
        """スケジュールされた更新処理を別スレッドで開始する (メインスレッドから呼ばれる)"""
        self.logger.info(
            f"run_scheduled_update が呼び出されました。Steps: {steps}"
        )  # ★ログ追加
        if self.is_updating:  # 念のため再度チェック
            self.logger.warning(
                "スケジュール実行しようとしましたが、既に更新処理が実行中でした。"
            )
            return

        self.is_updating = True
        status_text = (
            f"スケジュール更新 ({steps if steps != 'all' else '全ステップ'}) を開始..."
        )
        self.status_var.set(status_text)
        self.log_manager.log(f"{status_text}（バックグラウンド実行）")
        # ボタン無効化など
        self._disable_relevant_buttons()

        self.logger.info("スケジュール更新用スレッドを開始します。")  # ★ログ追加
        thread = threading.Thread(
            target=self._scheduled_update_thread, args=(steps,), daemon=True
        )
        thread.start()

    def _scheduled_update_thread(self, steps):
        """スケジュール更新を実行するワーカースレッド"""
        self.logger.info("_scheduled_update_thread が開始されました。")  # ★ログ追加
        update_success = False
        deploy_success = False  # スケジュール更新後にデプロイもするか？
        error_message = None
        results = {}  # results を初期化

        try:
            self.log_manager.log(f"実行ステップ: {steps}")
            if self.update_service:
                start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
                end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

                actual_steps = (
                    ["step1", "step2", "step3", "step4", "step5"]
                    if steps == "all"
                    else steps
                )
                if not isinstance(actual_steps, list):
                    self.log_manager.log(
                        f"ステップ指定が無効です: {actual_steps}", level=logging.ERROR
                    )
                    raise ValueError("ステップ指定が無効です")

                valid_steps = [
                    s
                    for s in actual_steps
                    if s in ["step1", "step2", "step3", "step4", "step5"]
                ]
                if not valid_steps:
                    self.log_manager.log(
                        f"有効な実行ステップがありません: {actual_steps}",
                        level=logging.WARNING,
                    )
                    raise ValueError("有効な実行ステップがありません")

                self.log_manager.log(
                    f"対象期間: {start_date} から {end_date}, 実行ステップ: {valid_steps}"
                )

                # update_period_step_by_step の呼び出し
                update_success, results = (
                    self.update_service.update_period_step_by_step(
                        start_date, end_date, valid_steps
                    )
                )

                # --- ★ 返り値のログを追加 ★ ---
                self.log_manager.log(
                    f"update_period_step_by_step の返り値: update_success={update_success}, results={results}"
                )
                # -----------------------------

                log_messages = []
                # --- ★ 結果の処理を修正: results['steps'] をループする ★ ---
                # results が辞書で、'steps' キーが存在し、その値も辞書であることを確認
                if (
                    isinstance(results, dict)
                    and "steps" in results
                    and isinstance(results["steps"], dict)
                ):
                    # update_success は Service からの全体結果を尊重しつつ、
                    # GUI側でのログ表示用に個別の結果も見る
                    actual_update_success = (
                        update_success  # Service からの全体成功フラグ
                    )

                    for step, result in results[
                        "steps"
                    ].items():  # ★ results['steps'].items() をループ
                        if isinstance(result, dict):
                            step_success = result.get(
                                "success", False
                            )  # 個別ステップの成功/失敗
                            status = "成功" if step_success else "失敗"
                            message = result.get("message", "メッセージなし")
                            count = result.get("count", 0)
                            log_messages.append(
                                f"ステップ {step}: {status} - {message} ({count}件処理)"
                            )
                            # 個別ステップが失敗なら、全体の成功フラグも False にする (念のため)
                            # if not step_success:
                            #     actual_update_success = False # 不要かも？ update_success をそのまま使う
                        else:
                            self.log_manager.log(
                                f"ステップ {step}: 結果の形式が不正です: {result}",
                                level=logging.WARNING,
                            )
                            actual_update_success = False  # 不正なら全体失敗
                            error_message = (
                                error_message or f"ステップ {step} の結果形式不正"
                            )

                    # ループ後の update_success は Service からの値を使うのが正しい
                    # ★ actual_update_success を update_success に反映 ★
                    update_success = actual_update_success

                else:
                    self.log_manager.log(
                        f"update_period_step_by_step の結果形式が不正です (results['steps'] が辞書でない等): {results}",
                        level=logging.ERROR,
                    )
                    update_success = False  # 不正なら失敗
                    error_message = error_message or "UpdateServiceからの結果形式不正"
                # ----------------------------------------------------

                for msg in log_messages:
                    self.log_manager.log(msg)

                if update_success:
                    self.log_manager.log("スケジュール更新処理が正常に完了しました。")
                    # 自動デプロイ実行 (変更なし)
                    auto_deploy = self.config.get_boolean(
                        "Deployment", "auto_deploy_after_update", fallback=False
                    )
                    if auto_deploy:
                        self.log_manager.log(
                            "スケジュール更新に伴う自動デプロイを開始します..."
                        )
                        frontend_db_path = self.config.get_value(
                            "Deployment", "frontend_db_path", fallback="frontend.duckdb"
                        )
                        try:
                            from database.frontend_database import FrontendDatabase
                            from services.deployment_service import DeploymentService

                            frontend_db = FrontendDatabase(
                                frontend_db_path, logger=self.logger
                            )
                            deployment_service = DeploymentService(
                                self.db_accessor, frontend_db, self.logger
                            )
                            deploy_success = deployment_service.deploy_all_tables()
                            if deploy_success:
                                self.log_manager.log(
                                    f"自動デプロイが完了しました: {frontend_db_path}"
                                )
                            else:
                                self.log_manager.log(
                                    "自動デプロイ処理中にエラーが発生しました。",
                                    level=logging.ERROR,
                                )
                                # ★ デプロイ失敗時もエラーメッセージを設定 ★
                                error_message = (
                                    error_message
                                    or "自動デプロイ処理中にエラーが発生しました。"
                                )
                        except ImportError as e:
                            self.log_manager.log(
                                f"デプロイに必要なモジュールのインポートに失敗: {e}",
                                level=logging.ERROR,
                            )
                            deploy_success = False  # デプロイ失敗
                            error_message = (
                                error_message or "デプロイモジュールのインポート失敗"
                            )
                        except Exception as e:
                            self.log_manager.log(
                                f"自動デプロイ処理中に予期せぬエラー: {e}",
                                level=logging.ERROR,
                            )
                            import traceback

                            self.log_manager.log(
                                f"スタックトレース: {traceback.format_exc()!r}",
                                level=logging.ERROR,
                            )
                            deploy_success = False  # デプロイ失敗
                            error_message = (
                                error_message or f"自動デプロイ中の予期せぬエラー: {e}"
                            )
                    else:
                        self.log_manager.log("自動デプロイは設定で無効化されています。")
                        deploy_success = True  # 不要なので成功扱い
                else:
                    self.log_manager.log(
                        "スケジュール更新処理に失敗または不正な結果が含まれていたため、デプロイはスキップされました。",
                        level=logging.WARNING,
                    )
                    # error_message が設定されていなければデフォルトのエラーメッセージを設定
                    error_message = (
                        error_message or "スケジュール更新処理が失敗しました。"
                    )

            else:
                # (変更なし)
                self.log_manager.log(
                    "エラー: UpdateService が利用できません。", level=logging.ERROR
                )
                error_message = "UpdateService が利用できません。"
                update_success = False
                deploy_success = False

        except Exception as e:
            # (変更なし)
            self.log_manager.log(
                f"スケジュール更新スレッドで予期せぬエラー: {e}", level=logging.ERROR
            )
            import traceback

            self.log_manager.log(
                f"スタックトレース: {traceback.format_exc()!r}", level=logging.ERROR
            )
            error_message = f"予期せぬエラー: {e}"
            update_success = False
            deploy_success = False
        finally:
            # update_success と deploy_success の両方を確認して最終ステータスを決定
            # (自動デプロイが有効で、更新が成功した場合のみ deploy_success が True になりうる)
            auto_deploy_enabled = self.config.get_boolean(
                "Deployment", "auto_deploy_after_update", fallback=False
            )
            # ★ デプロイが有効で、かつ更新かデプロイのどちらかが失敗したら final_success = False ★
            if auto_deploy_enabled:
                final_success = update_success and deploy_success
            else:
                final_success = update_success  # デプロイ無効なら更新結果のみで判断

            final_status = "完了" if final_success else "エラー"
            # ★ error_message を渡すように修正 ★
            self.root.after(
                0, self._update_gui_after_scheduled_thread, final_status, error_message
            )

    def _update_gui_after_scheduled_thread(self, final_status, error_msg=None):
        """スケジュール更新スレッド完了後にGUIを更新"""
        self.is_updating = False
        self.status_var.set(f"スケジュール更新 {final_status}")
        self.log_manager.log(f"スケジュール更新処理が {final_status} しました。")
        self._enable_relevant_buttons()  # 無効化したボタンを有効化
        if final_status == "エラー":
            # 詳細なエラーメッセージがあれば表示
            display_error = error_msg if error_msg else "処理中にエラーが発生しました。"
            messagebox.showerror(
                "スケジュール更新エラー", f"{display_error} ログを確認してください。"
            )
        # else:
        # 完了メッセージはログに出ているので不要かも
        # messagebox.showinfo("スケジュール更新完了", "スケジュールされた更新処理が完了しました。")

    def _disable_relevant_buttons(
        self, deploy=False, scheduler=False
    ):  # update, manual_update を削除
        """更新・デプロイ・スケジューラ実行中に関連ボタンを無効化"""
        if deploy or scheduler:
            # self.update_button.config(state=tk.DISABLED) # update_button は別管理 or 削除された想定
            if hasattr(self, "deploy_button"):
                self.deploy_button.config(state=tk.DISABLED)  # DuckDB更新ボタン
            # デプロイ中は他の操作もできなくする（コンフィグ、ステップ、日付など）
            if hasattr(self, "config_button"):
                self.config_button.config(state=tk.DISABLED)
            # ★ hasattr で存在チェックを追加 ★
            if hasattr(self, "step_checkboxes"):
                for cb in self.step_checkboxes:
                    cb.config(state=tk.DISABLED)
            if hasattr(self, "start_date_entry"):
                self.start_date_entry.config(state="disabled")
            if hasattr(self, "end_date_entry"):
                self.end_date_entry.config(state="disabled")
            if hasattr(self, "cup_id_entry"):
                self.cup_id_entry.config(state="disabled")

            # ★ スケジューラ関連ボタンも無効化 (存在する場合のみ) ★
            if (
                hasattr(self, "scheduler_control_frame")
                and self.scheduler_control_frame
            ):
                try:
                    # ... (ボタンの状態設定)
                    self.scheduler_control_frame.children["start_button"].config(
                        state=tk.DISABLED
                    )
                    self.scheduler_control_frame.children["stop_button"].config(
                        state=tk.DISABLED
                    )
                    self.scheduler_control_frame.children["add_button"].config(
                        state=tk.DISABLED
                    )
                    if "edit_button" in self.scheduler_control_frame.children:
                        self.scheduler_control_frame.children["edit_button"].config(
                            state=tk.DISABLED
                        )
                    if "delete_button" in self.scheduler_control_frame.children:
                        self.scheduler_control_frame.children["delete_button"].config(
                            state=tk.DISABLED
                        )
                except tk.TclError as e:
                    self._logger.warning(
                        f"スケジューラボタンの状態設定中にエラー: {e} (無視します)"
                    )
                except KeyError as e:
                    self._logger.warning(
                        f"スケジューラ制御フレームにボタンが見つかりません: {e}"
                    )

        # 実行完了後、プログレスラベルをデフォルトに戻す（少し待ってから）
        if hasattr(self, "update_progress_label"):
            self.root.after(
                1000, lambda: self.update_progress_label.config(text="待機中")
            )

    def _enable_relevant_buttons(self):
        """更新・デプロイ・スケジューラ完了時にボタンを有効化"""
        # ★ 他のどの処理も実行中でなければ有効化 ★
        if not self.deploy_running and not self.scheduler_running:
            # self.update_button.config(state=tk.NORMAL)
            if hasattr(self, "deploy_button"):
                self.deploy_button.config(state=tk.NORMAL)  # DuckDB更新ボタン
            # デプロイと関係ない他のコントロールも有効化
            if hasattr(self, "config_button"):
                self.config_button.config(state=tk.NORMAL)
            # ★ hasattr で存在チェックを追加 ★
            if hasattr(self, "step_checkboxes"):
                for cb in self.step_checkboxes:
                    cb.config(state=tk.NORMAL)
            if hasattr(self, "start_date_entry"):
                self.start_date_entry.config(state="normal")
            if hasattr(self, "end_date_entry"):
                self.end_date_entry.config(state="normal")
            if hasattr(self, "cup_id_entry"):
                self.cup_id_entry.config(state="normal")

            # ★ スケジューラ関連ボタンを有効化 (存在する場合のみ) ★
            if (
                hasattr(self, "scheduler_control_frame")
                and self.scheduler_control_frame
            ):
                try:
                    # スケジューラの状態に応じて Stop/Start を制御
                    scheduler_is_running = (
                        hasattr(self, "_scheduler_timer")
                        and self._scheduler_timer is not None
                    )
                    start_state = tk.DISABLED if scheduler_is_running else tk.NORMAL
                    stop_state = tk.NORMAL if scheduler_is_running else tk.DISABLED
                    self.scheduler_control_frame.children["start_button"].config(
                        state=start_state
                    )
                    self.scheduler_control_frame.children["stop_button"].config(
                        state=stop_state
                    )

                    self.scheduler_control_frame.children["add_button"].config(
                        state=tk.NORMAL
                    )

                    # Treeviewで項目が選択されているかどうかに応じてEdit/Deleteボタンの状態を制御
                    selected_items = []
                    if hasattr(self, "schedule_tree"):
                        selected_items = self.schedule_tree.selection()
                    edit_delete_state = tk.NORMAL if selected_items else tk.DISABLED
                    if "edit_button" in self.scheduler_control_frame.children:
                        self.scheduler_control_frame.children["edit_button"].config(
                            state=edit_delete_state
                        )
                    if "delete_button" in self.scheduler_control_frame.children:
                        self.scheduler_control_frame.children["delete_button"].config(
                            state=edit_delete_state
                        )

                except (
                    tk.TclError,
                    KeyError,
                    AttributeError,
                ) as e:  # 複数の例外をまとめてキャッチ
                    self._logger.warning(
                        f"スケジューラボタンまたはTreeviewの状態設定中にエラー: {e} (無視します)"
                    )

        # 実行完了後、プログレスラベルをデフォルトに戻す（少し待ってから）
        if hasattr(self, "update_progress_label"):
            self.root.after(
                1000, lambda: self.update_progress_label.config(text="待機中")
            )

    def _browse_db_file(self):
        """データベースファイルを選択するダイアログを表示"""
        file_path = filedialog.askopenfilename(
            title="データベースファイルを選択",
            filetypes=[
                ("SQLiteデータベース", "*.sqlite *.db"),
                ("すべてのファイル", "*.*"),
            ],
        )
        if file_path:
            self.db_path.set(file_path)
            self.log_manager.log(f"データベースファイルを変更: {file_path}")

    # ★★★ 追加: 単一日付セット処理 ★★★
    def _set_single_date(self):
        """単一日更新モードの日付エントリーから日付を取得してセットする"""
        try:
            # UIBuilder の DateEntry ウィジェットから日付を取得
            # UIBuilder 側に date_entry という名前で保存されていると仮定
            if hasattr(self.ui_builder, "date_entry") and isinstance(
                self.ui_builder.date_entry, DateEntry
            ):
                selected_date = (
                    self.ui_builder.date_entry.get_date()
                )  # dateオブジェクトを取得
                self.target_date = selected_date.strftime("%Y%m%d")
                self.logger.info(
                    f"単一日更新の日付をセットしました: {self.target_date}"
                )
            else:
                self.logger.warning(
                    "日付入力ウィジェット (date_entry) が UIBuilder に見つかりません。"
                )
        except Exception as e:
            self.logger.error(f"日付のセット中にエラーが発生しました: {e}")
            messagebox.showerror("エラー", f"日付のセットに失敗しました。\n{e}")

    # ★★★ 追加: 期間日付セット処理 ★★★
    def _set_period_dates(self):
        """期間更新モードの日付エントリーから開始日と終了日を取得してセットする"""
        try:
            # UIBuilder の開始日と終了日の DateEntry ウィジェットから日付を取得
            # UIBuilder 側に start_date_entry, end_date_entry という名前で保存されていると仮定
            if (
                hasattr(self.ui_builder, "start_date_entry")
                and isinstance(self.ui_builder.start_date_entry, DateEntry)
                and hasattr(self.ui_builder, "end_date_entry")
                and isinstance(self.ui_builder.end_date_entry, DateEntry)
            ):

                start_date_obj = self.ui_builder.start_date_entry.get_date()
                end_date_obj = self.ui_builder.end_date_entry.get_date()

                # 開始日 <= 終了日 かチェック
                if start_date_obj > end_date_obj:
                    messagebox.showerror(
                        "日付エラー", "開始日は終了日より後の日付にできません。"
                    )
                    return

                self.start_date = start_date_obj.strftime("%Y%m%d")
                self.end_date = end_date_obj.strftime("%Y%m%d")
                self.logger.info(
                    f"期間更新の日付をセットしました: {self.start_date} - {self.end_date}"
                )
            else:
                self.logger.warning(
                    "期間入力ウィジェット (start_date_entry/end_date_entry) が UIBuilder に見つかりません。"
                )
        except Exception as e:
            self.logger.error(f"期間のセット中にエラーが発生しました: {e}")
            messagebox.showerror("エラー", f"期間のセットに失敗しました。\n{e}")

    def _update_options_log(self):
        # ログ出力用の簡単なメソッド (必要に応じて調整)
        # ★★★ 修正: step_vars_map をここで定義 ★★★
        step_vars_map = {
            "開催": self.fetch_cups,
            "詳細": self.fetch_cup_details,
            "レース": self.fetch_race_data,
            "オッズ": self.fetch_odds_data,
            "Yenjoy/結果": self.fetch_yenjoy_results,
        }
        # selected_steps = [ # Unused local variable removed
        # name
        # for name, var in step_vars_map.items()
        # if isinstance(var, tk.BooleanVar) and var.get()
        # ]
        # ★★★ 修正: 定義した step_vars_map を使用 ★★★
        log_items = []
        for k, v in step_vars_map.items():
            if isinstance(v, tk.BooleanVar):
                log_items.append(f"{k}={v.get()}")
            else:
                # BooleanVar 以外の場合のフォールバック (エラー防止)
                log_items.append(f"{k}=?")
        self.logger.info(f"更新オプションを設定しました: {', '.join(log_items)}")


def main():
    """アプリケーションのメインエントリーポイント"""
    try:
        # GUIの作成と実行
        root = tk.Tk()
        app = KeirinUpdaterGUI(root)
        root.mainloop()
    except Exception as e:
        # 未処理の例外をキャッチ
        print(f"アプリケーション実行中に致命的なエラーが発生しました: {str(e)}")
        # ロギングがセットアップされていれば詳細をログに記録
        if "app" in locals() and hasattr(app, "log_manager"):
            app.log_manager.log_error("アプリケーションの致命的なエラー", e)

        # tkinterのエラーメッセージを表示
        import traceback

        error_message = f"エラーが発生しました:\n{str(e)}\n\n{traceback.format_exc()}"
        try:
            if "root" in locals() and root:
                tk.messagebox.showerror("致命的なエラー", error_message)
        except Exception:
            # フォールバック：コンソールにエラーを表示
            print(error_message)

        # 強制終了
        if "root" in locals() and root:
            root.destroy()
        sys.exit(1)


if __name__ == "__main__":
    main()
