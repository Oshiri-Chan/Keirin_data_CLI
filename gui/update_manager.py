"""
更新処理を管理するクラス
"""

import glob
import logging
import os
import threading
import time
import tkinter.messagebox as messagebox
from datetime import datetime

from api.winticket_api import WinticketAPI
from api.yenjoy_api import YenjoyAPI
from database.db_accessor import KeirinDataAccessor
from services.update_service import UpdateService
from utils.config_manager import ConfigManager


class UpdateManager:
    """
    更新処理を管理するクラス
    """

    def __init__(
        self,
        db_accessor: KeirinDataAccessor,
        logger: logging.Logger,
        controller,
        config_manager: ConfigManager,
    ):
        """
        初期化

        Args:
            logger (logging.Logger): ロガーインスタンス
            controller: GUIコントローラーのインスタンス
            config_manager (ConfigManager): 設定マネージャーインスタンス
        """
        self.logger = logger
        self.controller = controller
        self.config_manager = config_manager
        self.update_thread = None
        self.is_cancelled = False
        self._lock = threading.Lock()

        # APIインスタンスの初期化
        self.winticket_api = WinticketAPI(logger=self.logger)
        self.yenjoy_api = YenjoyAPI(logger=self.logger)

        # 渡されたKeirinDataAccessorを使用
        self.db_accessor = db_accessor

        # 更新サービスの初期化
        if self.db_accessor:
            self.update_service = UpdateService(
                winticket_api=self.winticket_api,
                yenjoy_api=self.yenjoy_api,
                db_accessor=self.db_accessor,
                logger=self.logger,
                config_manager=self.config_manager,
                default_max_workers=self.config_manager.get_int(
                    "PERFORMANCE", "max_workers", fallback=5
                ),
                winticket_rate_limit_wait=self.config_manager.get_float(
                    "PERFORMANCE", "rate_limit_winticket", fallback=0.1
                ),
                yenjoy_rate_limit_wait_html=self.config_manager.get_float(
                    "PERFORMANCE", "rate_limit_yenjoy_html", fallback=1.0
                ),
                yenjoy_rate_limit_wait_api=self.config_manager.get_float(
                    "PERFORMANCE", "rate_limit_yenjoy_api", fallback=1.0
                ),
            )
        else:
            self.update_service = None

        # データベース初期化関連
        self._db_initializer = None
        self._db_initialized = False

        # スレッド制御用
        self._update_thread = None
        self._stop_event = threading.Event()
        self._thread_lock = threading.Lock()

        # 更新オプション
        self.fetch_cups = True
        self.fetch_cup_details = True
        self.fetch_race_data = True
        self.fetch_odds_data = True
        self.fetch_yenjoy_results = True

    def validate_inputs(self, date_str, db_path):
        """
        入力値の検証

        Args:
            date_str (str): 日付文字列（YYYYMMDD）
            db_path (str): データベースファイルパス

        Returns:
            bool: 検証結果
        """
        # データベースパスの検証
        if not db_path:
            self.logger.error("データベースパスが設定されていません")
            return False

        # DBイニシャライザの取得とパス検証
        db_initializer = self.controller.db_initializer
        if not db_initializer.validate_db_path(db_path):
            return False

        # 日付の検証
        if not date_str or len(date_str) != 8:
            self.logger.error("日付の形式が不正です (YYYYMMDD)")
            return False

        try:
            # 日付の形式を検証
            datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            self.logger.error(f"無効な日付です: {date_str}")
            return False

        # Winticketかyenjoyのどちらかが選択されているか検証
        if not (
            self.controller.update_winticket.get()
            or self.controller.update_yenjoy.get()
        ):
            self.logger.error("更新するデータが選択されていません")
            return False

        return True

    def start_update(self, mode, date_str, start_date=None, end_date=None):
        """
        更新処理の開始

        Args:
            mode (str): 更新モード ('single', 'period', 'all')
            date_str (str): 日付文字列（YYYYMMDD）
            start_date (datetime.date, optional): 開始日
            end_date (datetime.date, optional): 終了日
        """
        try:
            # ログ出力を強化
            self.logger.info(f"更新処理を開始します。モード: {mode}, 日付: {date_str}")

            # 更新中フラグをセット
            self.controller.is_updating = True

            # UI表示の更新
            self.controller.ui_builder.update_progress(True, "更新を準備中...")

            # データベースパスのログ出力
            db_path = self.controller.db_path.get()
            self.logger.debug(f"データベースパス: {db_path}")

            # 更新オプションをコントローラから取得
            if hasattr(self.controller, "fetch_cups"):
                self.fetch_cups = self.controller.fetch_cups.get()
            if hasattr(self.controller, "fetch_cup_details"):
                self.fetch_cup_details = self.controller.fetch_cup_details.get()
            if hasattr(self.controller, "fetch_race_data"):
                self.fetch_race_data = self.controller.fetch_race_data.get()
            if hasattr(self.controller, "fetch_odds_data"):
                self.fetch_odds_data = self.controller.fetch_odds_data.get()
            if hasattr(self.controller, "fetch_yenjoy_results"):
                self.fetch_yenjoy_results = self.controller.fetch_yenjoy_results.get()

            self.logger.info(
                f"更新設定 - 開催情報: {self.fetch_cups}, 開催詳細: {self.fetch_cup_details}, "
                f"レース情報: {self.fetch_race_data}, オッズ情報: {self.fetch_odds_data}, "
                f"Yenjoy結果: {self.fetch_yenjoy_results}"
            )

            # 既存のスレッドが存在し、実行中の場合は終了を待つ
            if (
                hasattr(self, "_update_thread")
                and self._update_thread
                and self._update_thread.is_alive()
            ):
                self.logger.warning(
                    "前回の更新スレッドが実行中です。終了を待機します..."
                )
                # 最大10秒待機
                for _ in range(100):
                    if not self._update_thread.is_alive():
                        break
                    time.sleep(0.1)

                # それでも終了しない場合は警告
                if self._update_thread.is_alive():
                    self.logger.warning(
                        "前回の更新スレッドが終了しませんでした。新しいスレッドを開始します。"
                    )

            # バックグラウンドスレッドで更新処理を実行
            self._update_thread = threading.Thread(
                target=self._run_update,
                args=(mode, date_str, start_date, end_date),
                daemon=True,
            )

            # スレッド名を設定
            self._update_thread.name = (
                f"UpdateThread-{datetime.now().strftime('%H%M%S')}"
            )

            # スレッドの開始
            self._update_thread.start()

            # スレッド状態をログに出力
            self.logger.info(f"更新スレッドを開始しました: {self._update_thread.name}")

        except Exception as e:
            self.logger.error(
                f"更新処理の開始中にエラーが発生しました: {str(e)}", exc_info=True
            )
            self.controller.is_updating = False
            self.controller.ui_builder.update_progress(False, "更新失敗")

    def _run_update(self, mode, date_str, start_date=None, end_date=None):
        """
        更新処理を実行（更新スレッド内で呼び出される）

        Args:
            mode (str): 更新モード（single, period, all）
            date_str (str): 更新対象日（単一日更新の場合）
            start_date (datetime.date, optional): 開始日（期間更新の場合）
            end_date (datetime.date, optional): 終了日（期間更新の場合）
        """
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"スレッド {thread_id}: 更新スレッドが開始されました。開始時刻: {datetime.now()}"
        )

        success = False
        try:
            # 進捗コールバック
            def progress_callback(step, step_index, total_steps, message):
                # step_indexがdict型の場合の対応（APIの変更により発生する可能性がある）
                if isinstance(step_index, dict):
                    # 辞書の場合は単にstepの文字列と結果メッセージを使用
                    self.controller.root.after_idle(
                        lambda: self._update_status(f"{step}: {message}")
                    )
                else:
                    # GUI更新はメインスレッドで行う必要がある
                    self.controller.root.after_idle(
                        lambda: self._update_progress(
                            step, step_index, total_steps, message
                        )
                    )

            # パラメータを取得
            steps = []  # デフォルトは空リスト

            # ユーザーが選択したステップに基づいてステップリストを作成
            if self.fetch_cups:
                steps.append("step1")
            if self.fetch_cup_details:
                steps.append("step2")
            if self.fetch_race_data:
                steps.append("step3")
            if self.fetch_odds_data:
                steps.append("step4")
            if self.fetch_yenjoy_results:
                steps.append("step5")

            # ステップが何も選択されていなければエラーメッセージを表示
            if not steps:
                error_msg = "実行するステップが選択されていません。更新オプションで少なくとも1つのステップを選択してください。"
                self.logger.error(error_msg)
                self.controller.root.after_idle(lambda: self._update_status(error_msg))
                self.controller.root.after_idle(lambda: self._update_completed(False))
                return

            # 開始メッセージをGUIに表示
            message = (
                f"更新開始: {start_date} から {end_date} (選択済みステップ: {steps})"
            )
            self.controller.root.after_idle(lambda: self._update_status(message))

            # 作業用ディレクトリを作成
            temp_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "temp"
            )
            os.makedirs(temp_dir, exist_ok=True)

            # 更新処理を実行
            try:
                # MySQL移行により、初期データ抽出は不要
                # （KeirinDataAccessorが直接データベースから必要な情報を取得）

                # ステップバイステップ更新を実行
                success, result = self.update_service.update_period_step_by_step(
                    start_date, end_date, steps=steps, callback=progress_callback
                )

                # 結果に応じてステータスを更新
                if success:
                    self.controller.root.after_idle(
                        lambda: self._update_status(f"更新完了: {result}")
                    )
                else:
                    self.controller.root.after_idle(
                        lambda: self._update_status(f"更新失敗: {result}")
                    )

                # 一時ファイルのクリーンアップ
                try:
                    files = glob.glob(os.path.join(temp_dir, "*"))
                    for file in files:
                        try:
                            if os.path.isfile(file):
                                os.remove(file)
                                self.logger.info(f"一時ファイルを削除しました: {file}")
                        except Exception as e:
                            self.logger.warning(
                                f"一時ファイルの削除に失敗: {file} - {e}"
                            )
                except Exception as e:
                    self.logger.warning(f"一時ファイルのクリーンアップに失敗: {e}")

            except Exception as e:
                self.logger.error(
                    f"更新処理の実行中にエラーが発生しました: {str(e)}", exc_info=True
                )
                error_str = str(e)
                self.controller.root.after_idle(
                    lambda err=error_str: self._update_status(f"エラー: {err}")
                )
                success = False

            # 更新完了時の処理
            self.controller.root.after_idle(lambda: self._update_completed(success))

        except Exception as e:
            self.logger.error(
                f"更新スレッドでエラーが発生しました: {str(e)}", exc_info=True
            )
            self.controller.root.after_idle(lambda: self._update_completed(False))

        finally:
            self.logger.info(
                f"スレッド {thread_id}: 更新スレッドが終了しました。終了時刻: {datetime.now()}"
            )
            # スレッド終了時のクリーンアップ
            self.controller.is_updating = False

    def _update_progress(self, step, step_index, total_steps, message):
        """
        進捗状況を更新

        Args:
            step (str): 更新ステップ名
            step_index (int): 現在のステップインデックス
            total_steps (int): 全ステップ数
            message (str): 進捗メッセージ
        """
        # 進捗割合の計算（ステップベース）
        if total_steps > 0:
            progress = int((step_index / total_steps) * 100)
        else:
            progress = 0

        # ステータス表示の更新
        status_msg = f"ステップ {step_index}/{total_steps} ({step}): {message}"
        self.controller.root.after_idle(
            lambda: self._update_status(status_msg, progress)
        )

        # UIのプログレスバーを更新
        if hasattr(self.controller, "root") and hasattr(self.controller.root, "after"):
            self.controller.root.after_idle(
                lambda: self.controller.ui_builder.update_progress_bar(progress)
            )

    def _update_completed(self, success):
        """
        更新完了時の処理

        Args:
            success (bool): 更新が成功したかどうか
        """
        # UIスレッドで実行
        if hasattr(self.controller, "root") and hasattr(self.controller.root, "after"):
            self.controller.root.after(0, lambda: self._update_completed_ui(success))

    def _update_completed_ui(self, success):
        """
        更新完了時のUI更新処理（UIスレッドで実行）

        Args:
            success (bool): 更新が成功したかどうか
        """
        # 更新中フラグをリセット
        self.controller.is_updating = False

        # UI表示の更新
        if success:
            self.controller.ui_builder.update_progress(False, "更新完了")
            self.logger.info("更新処理が正常に完了しました")
        else:
            self.controller.ui_builder.update_progress(False, "更新失敗")
            self.logger.error("更新処理が失敗しました")

        # 自動更新が有効な場合は次回の更新をスケジュール
        if (
            hasattr(self.controller, "auto_update")
            and self.controller.auto_update.get()
        ):
            self.controller._schedule_auto_update()

    def _update_status(self, message, progress=None):
        """
        ステータス表示の更新

        Args:
            message (str): ステータスメッセージ
            progress (int, optional): 進捗率
        """
        # ログにメッセージを出力
        self.logger.info(f"ステータス: {message}")

        # 進捗率が指定されている場合はプログレスバーも更新
        if progress is not None:
            self.logger.debug(f"進捗率: {progress}%")

        # UIスレッドで実行
        if hasattr(self.controller, "root") and hasattr(self.controller.root, "after"):
            self.controller.root.after(
                0, lambda: self._update_status_ui(message, progress)
            )

    def _update_status_ui(self, message, progress=None):
        """
        UIスレッドでのステータス表示更新

        Args:
            message (str): ステータスメッセージ
            progress (int, optional): 進捗率
        """
        # ステータス表示を更新
        if hasattr(self.controller, "status_var"):
            self.controller.status_var.set(message)

        # ログに追加
        if hasattr(self.controller, "log_manager"):
            self.controller.log_manager.log(message)

        # 進捗率が指定されている場合はプログレスバーも更新
        if progress is not None and hasattr(self.controller, "ui_builder"):
            self.controller.ui_builder.update_progress_bar(progress)

    def cancel_update(self):
        """
        更新処理のキャンセル
        """
        with self._thread_lock:
            if not self._update_thread or not self._update_thread.is_alive():
                self.logger.warning("キャンセル可能な更新処理が実行されていません")
                return False

            self.logger.info("更新処理のキャンセルを開始します")
            self.is_cancelled = True
            self._stop_event.set()

            # キャンセル中のメッセージを表示
            self._update_status("更新処理をキャンセル中...")

            return True

    def update_options(
        self,
        fetch_cups=True,
        fetch_cup_details=True,
        fetch_race_data=True,
        fetch_odds_data=True,
        fetch_yenjoy_results=True,
    ):
        """
        更新オプションの設定

        Args:
            fetch_cups (bool): 開催情報を取得するかどうか
            fetch_cup_details (bool): 開催詳細を取得するかどうか
            fetch_race_data (bool): レース情報を取得するかどうか
            fetch_odds_data (bool): オッズ情報を取得するかどうか
            fetch_yenjoy_results (bool): Yenjoy結果を取得するかどうか
        """
        self.fetch_cups = fetch_cups
        self.fetch_cup_details = fetch_cup_details
        self.fetch_race_data = fetch_race_data
        self.fetch_odds_data = fetch_odds_data
        self.fetch_yenjoy_results = fetch_yenjoy_results

        self.logger.info(
            f"更新オプションを設定しました: 開催={fetch_cups}, 詳細={fetch_cup_details}, レース={fetch_race_data}, オッズ={fetch_odds_data}, Yenjoy={fetch_yenjoy_results}"
        )

    def stop_update(self):
        """
        更新処理の停止（リソース解放）
        """
        self._stop_event.set()
        if self._update_thread and self._update_thread.is_alive():
            try:
                self._update_thread.join(timeout=3.0)
            except Exception as e:
                self.logger.error(f"更新スレッドの終了を待機中にエラー: {str(e)}")

        # リソース解放
        if hasattr(self, "update_service"):
            self.logger.info("更新サービスのリソースを解放します")
            # 必要に応じてリソース解放処理を追加

        self.logger.info("更新マネージャーのリソースを解放しました")
