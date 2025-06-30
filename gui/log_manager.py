"""
ログ管理を担当するモジュール
"""

import logging
import sys
import tkinter as tk
import traceback
from datetime import datetime
from pathlib import Path
from queue import Queue
from threading import Lock


class LogManager:
    """
    ログ管理を責務とするクラス
    スレッドセーフな実装
    """

    def __init__(self, controller, log_level=logging.INFO):
        """
        初期化

        Args:
            controller: GUIコントローラーオブジェクト
            log_level: ログレベル（デフォルトINFO）
        """
        self.controller = controller

        # ログディレクトリの初期化
        self.log_dir = Path("logs")
        self.log_dir.mkdir(exist_ok=True)

        # ロガーの初期化
        self.logger = self._setup_logger(log_level)

        # GUIログ用のキュー
        self.log_queue = Queue()

        # スレッド安全性のためのロック
        self.log_lock = Lock()

        # GUIログ処理を開始
        self._start_log_processor()

    # 互換性のためのメソッド追加
    def get_logger(self):
        """
        loggerプロパティの互換性のために提供するメソッド

        Returns:
            logging.Logger: ロガーオブジェクト
        """
        return self.logger

    def _setup_logger(self, log_level):
        """
        ロガーのセットアップ

        Args:
            log_level: ログレベル

        Returns:
            logging.Logger: セットアップ済みのロガー
        """
        # ロガーの作成
        logger = logging.getLogger("keirin_updater")
        logger.setLevel(log_level)

        # すでにハンドラが設定されている場合はクリア
        if logger.hasHandlers():
            logger.handlers.clear()

        # コンソールへの出力設定
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_format = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(console_format)
        logger.addHandler(console_handler)

        # ファイルへの出力設定
        try:
            today = datetime.now().strftime("%Y%m%d")
            log_file = self.log_dir / f"keirin_updater_{today}.log"
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(log_level)
            file_format = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(module)s - %(message)s",
                "%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(file_format)
            logger.addHandler(file_handler)
        except Exception as e:
            # ファイルログ設定失敗時はコンソールに出力して続行
            print(f"ファイルログの設定に失敗しました: {str(e)}")

        return logger

    def _start_log_processor(self):
        """ログキュー処理を開始"""
        if hasattr(self.controller, "root"):
            # 100ミリ秒ごとにキューを処理
            self.controller.root.after(100, self._process_log_queue)

    def _process_log_queue(self):
        """ログキューの処理（メインスレッドで実行）"""
        try:
            # キューの項目を最大10件まで処理
            for _ in range(10):
                if self.log_queue.empty():
                    break
                message = self.log_queue.get_nowait()
                self._update_gui_log(message)
                self.log_queue.task_done()
        except Exception as e:
            print(f"ログキュー処理エラー: {str(e)}")
        finally:
            # 次の処理をスケジュール
            if hasattr(self.controller, "root"):
                self.controller.root.after(100, self._process_log_queue)

    def log(self, message, level=logging.INFO):
        """
        GUIのログ領域とロガーの両方にメッセージを出力（スレッドセーフ）

        Args:
            message: 出力するメッセージ
            level: ログレベル（デフォルトINFO）またはレベル名
        """
        with self.log_lock:
            try:
                # レベルが文字列で指定された場合の変換
                if isinstance(level, str):
                    level = self._get_level_from_string(level)

                # ロガーへの出力
                if level == logging.DEBUG:
                    self.logger.debug(message)
                elif level == logging.INFO:
                    self.logger.info(message)
                elif level == logging.WARNING:
                    self.logger.warning(message)
                elif level == logging.ERROR:
                    self.logger.error(message)
                elif level == logging.CRITICAL:
                    self.logger.critical(message)

                # GUIログ表示キューに追加
                self.log_queue.put(message)

            except Exception as e:
                # フォールバック：コンソールに直接出力
                print(f"ログ出力エラー: {message} - {str(e)}")

    def _get_level_from_string(self, level_name):
        """文字列からログレベルを取得"""
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        return level_map.get(level_name.upper(), logging.INFO)

    def _update_gui_log(self, message):
        """
        GUIのログ領域にメッセージを追加（メインスレッドから呼び出し）

        Args:
            message: 表示するメッセージ
        """
        try:
            # GUIが初期化されているか確認
            if not hasattr(self.controller, "ui_builder") or not hasattr(
                self.controller.ui_builder, "log_text"
            ):
                return

            log_text = self.controller.ui_builder.log_text
            # 現在時刻を取得
            current_time = datetime.now().strftime("%H:%M:%S")
            # メッセージをGUIに追加
            log_text.config(state=tk.NORMAL)  # 編集可能にする
            log_text.insert(tk.END, f"[{current_time}] {message}\n")
            # 行数を制限（最大1000行）
            num_lines = int(log_text.index("end-1c").split(".")[0])
            if num_lines > 1000:
                log_text.delete("1.0", "101.0")
            log_text.see(tk.END)  # 最下部にスクロール
            log_text.config(state=tk.DISABLED)  # 編集不可に戻す
        except Exception as e:
            # エラーが発生した場合はコンソールに出力
            print(f"GUIログ表示エラー: {str(e)}")

    def log_error(self, message, exception=None):
        """
        エラーメッセージをログに出力（スレッドセーフ）

        Args:
            message: エラーメッセージ
            exception: 例外オブジェクト（指定時は例外情報も出力）
        """
        with self.log_lock:
            try:
                if exception:
                    # エラーメッセージの作成
                    error_message = f"{message}: {str(exception)}"

                    # スタックトレースをファイルログに記録
                    stack_trace = "".join(
                        traceback.format_exception(
                            type(exception), exception, exception.__traceback__
                        )
                    )
                    self.logger.error(f"{error_message}\n{stack_trace}")

                    # GUIログには概要のみ表示
                    self.log_queue.put(error_message)
                else:
                    # 例外なしの場合は通常のエラーログ
                    self.logger.error(message)
                    self.log_queue.put(message)
            except Exception as e:
                # フォールバック：コンソールに直接出力
                print(f"エラーログ出力エラー: {message} - {str(e)}")

    def clear_log(self):
        """GUIのログ表示領域をクリア（スレッドセーフ）"""
        try:
            # メインスレッドでの実行を保証
            if hasattr(self.controller, "root"):
                self.controller.root.after(0, self._clear_gui_log)
        except Exception as e:
            print(f"ログクリア要求エラー: {str(e)}")

    def _clear_gui_log(self):
        """GUIログの実際のクリア処理（メインスレッドから呼び出し）"""
        try:
            if hasattr(self.controller, "ui_builder") and hasattr(
                self.controller.ui_builder, "log_text"
            ):
                log_text = self.controller.ui_builder.log_text
                log_text.config(state=tk.NORMAL)
                log_text.delete(1.0, tk.END)
                log_text.config(state=tk.DISABLED)
                self.log("ログをクリアしました")
        except Exception as e:
            print(f"ログクリア実行エラー: {str(e)}")

    def shutdown(self):
        """ロギングシステムの終了処理"""
        try:
            # ロガーのハンドラをクローズ
            for handler in self.logger.handlers:
                handler.close()
                self.logger.removeHandler(handler)

            # キューに残っているログを処理
            while not self.log_queue.empty():
                try:
                    self.log_queue.get_nowait()
                    self.log_queue.task_done()
                except:
                    pass

            logging.shutdown()
        except Exception as e:
            print(f"ログシャットダウンエラー: {str(e)}")
