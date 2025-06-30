"""
競輪データ更新アプリケーションの設定を管理するモジュール
"""

import configparser
import logging
import os
from datetime import datetime, timedelta


class Config:
    """
    設定を管理するクラス

    設定ファイルからの読み込みと保存、デフォルト値の提供を行う
    """

    def __init__(self, config_file=None, logger=None):
        """
        初期化

        Args:
            config_file (str): 設定ファイルのパス（省略時はデフォルト）
            logger: ロガーオブジェクト（省略時は標準ロガー）
        """
        self.logger = logger or logging.getLogger(__name__)

        # デフォルト設定ファイルのパスを設定
        if config_file is None:
            # config.py と同じディレクトリにある config.ini をデフォルトにする
            self.config_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "config.ini"
            )
        else:
            self.config_file = config_file

        self.logger.debug(f"設定ファイル: {self.config_file}")

        # 設定値の初期化
        self._init_default_values()

        # 設定ファイルがあれば読み込む
        if os.path.exists(self.config_file):
            self.load()
        else:
            self.logger.info(
                f"設定ファイルが見つかりません。デフォルト値を使用します: {self.config_file}"
            )
            # 初期設定を保存
            self.save()

    def _init_default_values(self):
        """デフォルト設定値を初期化"""
        # デフォルト値の設定
        # プロジェクトルートの取得は不要（MySQL接続のため）
        self.db_path = "MySQL Database (Connection managed by KeirinDataAccessor)"
        self.log_level = "INFO"
        self.max_log_files = 10

        # PERFORMANCE 設定 (デフォルト値)
        self.max_workers = 5
        self.rate_limit_winticket = 1.0
        self.rate_limit_yenjoy = 1.0
        self.step3_max_workers = 1
        self.saver_batch_size = 50

        # 更新設定
        self.fetch_cups = True
        self.fetch_cup_details = True
        self.fetch_race_data = True
        self.fetch_odds_data = True
        self.fetch_yenjoy_results = True
        self.auto_update = False
        self.auto_update_interval = 24  # 時間単位
        self.update_winticket = True
        self.update_yenjoy = True
        self.default_update_mode = "single"  # single, period, all

        # APIリクエスト設定
        self.request_timeout = 30  # 秒
        self.retry_count = 3
        self.retry_delay = 5  # 秒

        # 日付設定
        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        self.last_update_date = today
        self.default_date = today

        # GUI設定 (デフォルト)
        self.last_update_mode = "single"
        self.last_target_date = yesterday
        self.last_start_date = yesterday
        self.last_end_date = today
        self.last_until_now = True

    def load(self):
        """設定ファイルから設定を読み込む"""
        try:
            self.logger.debug(f"設定ファイルを読み込みます: {self.config_file}")

            config = configparser.ConfigParser()
            config.read(self.config_file, encoding="utf-8")

            # システム設定
            if "System" in config:
                system = config["System"]
                self.db_path = system.get("db_path", self.db_path)
                self.log_level = system.get("log_level", self.log_level).upper()
                self.max_log_files = system.getint("max_log_files", self.max_log_files)

            # PERFORMANCE設定の読み込み
            if "PERFORMANCE" in config:
                performance = config["PERFORMANCE"]
                self.max_workers = performance.getint("max_workers", self.max_workers)
                self.rate_limit_winticket = performance.getfloat(
                    "rate_limit_winticket", self.rate_limit_winticket
                )
                self.rate_limit_yenjoy = performance.getfloat(
                    "rate_limit_yenjoy", self.rate_limit_yenjoy
                )
                self.step3_max_workers = performance.getint(
                    "step3_max_workers", self.step3_max_workers
                )
                self.saver_batch_size = performance.getint(
                    "saver_batch_size", self.saver_batch_size
                )
            else:
                # PERFORMANCEセクションがない場合はデフォルト値が使われる
                # _init_default_valuesで設定済みのため、ここでは特に何もしない
                pass

            # 更新設定
            if "Update" in config:
                update = config["Update"]
                self.auto_update = update.getboolean("auto_update", self.auto_update)
                self.auto_update_interval = update.getint(
                    "auto_update_interval", self.auto_update_interval
                )
                self.update_winticket = update.getboolean(
                    "update_winticket", self.update_winticket
                )
                self.update_yenjoy = update.getboolean(
                    "update_yenjoy", self.update_yenjoy
                )
                self.default_update_mode = update.get(
                    "default_update_mode", self.default_update_mode
                )
                # Fetch flags
                self.fetch_cups = update.getboolean("fetch_cups", self.fetch_cups)
                self.fetch_cup_details = update.getboolean(
                    "fetch_cup_details", self.fetch_cup_details
                )
                self.fetch_race_data = update.getboolean(
                    "fetch_race_data", self.fetch_race_data
                )
                self.fetch_odds_data = update.getboolean(
                    "fetch_odds_data", self.fetch_odds_data
                )
                self.fetch_yenjoy_results = update.getboolean(
                    "fetch_yenjoy_results", self.fetch_yenjoy_results
                )

            # APIリクエスト設定
            if "API" in config:
                api = config["API"]
                self.request_timeout = api.getint(
                    "request_timeout", self.request_timeout
                )
                self.retry_count = api.getint("retry_count", self.retry_count)
                self.retry_delay = api.getint("retry_delay", self.retry_delay)

            # 日付設定
            if "Date" in config:
                date = config["Date"]
                self.last_update_date = date.get(
                    "last_update_date", self.last_update_date
                )
                self.default_date = date.get("default_date", self.default_date)

            # GUI設定
            if "GUI" in config:
                gui = config["GUI"]
                self.last_update_mode = gui.get(
                    "last_update_mode", self.last_update_mode
                )
                self.last_target_date = gui.get(
                    "last_target_date", self.last_target_date
                )
                self.last_start_date = gui.get("last_start_date", self.last_start_date)
                self.last_end_date = gui.get("last_end_date", self.last_end_date)
                self.last_until_now = gui.getboolean(
                    "last_until_now", self.last_until_now
                )

            self.logger.info("設定ファイルを正常に読み込みました")
        except Exception as e:
            self.logger.error(f"設定ファイルの読み込み中にエラーが発生しました: {e}")
            import traceback

            self.logger.error(f"詳細: {traceback.format_exc()}")
            # エラー時はデフォルト値を使用
            self._init_default_values()

    def save(self):
        """設定ファイルに現在の設定を保存"""
        try:
            self.logger.debug(f"設定ファイルに保存します: {self.config_file}")

            config = configparser.ConfigParser()

            # システム設定
            config["System"] = {
                "db_path": self.db_path,
                "log_level": self.log_level,
                "max_log_files": str(self.max_log_files),
            }

            # PERFORMANCE設定
            config["PERFORMANCE"] = {
                "max_workers": str(self.max_workers),
                "rate_limit_winticket": str(self.rate_limit_winticket),
                "rate_limit_yenjoy": str(self.rate_limit_yenjoy),
                "step3_max_workers": str(self.step3_max_workers),
                "saver_batch_size": str(self.saver_batch_size),
            }

            # 更新設定
            config["Update"] = {
                "auto_update": str(self.auto_update),
                "auto_update_interval": str(self.auto_update_interval),
                "update_winticket": str(self.update_winticket),
                "update_yenjoy": str(self.update_yenjoy),
                "default_update_mode": self.default_update_mode,
                # Fetch flags
                "fetch_cups": str(self.fetch_cups),
                "fetch_cup_details": str(self.fetch_cup_details),
                "fetch_race_data": str(self.fetch_race_data),
                "fetch_odds_data": str(self.fetch_odds_data),
                "fetch_yenjoy_results": str(self.fetch_yenjoy_results),
            }

            # APIリクエスト設定
            config["API"] = {
                "request_timeout": str(self.request_timeout),
                "retry_count": str(self.retry_count),
                "retry_delay": str(self.retry_delay),
            }

            # 日付設定
            config["Date"] = {
                "last_update_date": self.last_update_date,
                "default_date": self.default_date,
            }

            # GUI設定
            config["GUI"] = {
                "last_update_mode": self.last_update_mode,
                "last_target_date": self.last_target_date,
                "last_start_date": self.last_start_date,
                "last_end_date": self.last_end_date,
                "last_until_now": str(self.last_until_now),
            }

            # ディレクトリの確認と作成
            config_dir = os.path.dirname(self.config_file)
            if config_dir and not os.path.exists(config_dir):
                os.makedirs(config_dir, exist_ok=True)
                self.logger.debug(
                    f"設定ファイルのディレクトリを作成しました: {config_dir}"
                )

            # 設定ファイルに書き込み
            with open(self.config_file, "w", encoding="utf-8") as configfile:
                config.write(configfile)

            self.logger.info(f"設定ファイルを保存しました: {self.config_file}")
        except Exception as e:
            self.logger.error(f"設定ファイルの保存中にエラーが発生しました: {e}")
            import traceback

            self.logger.error(f"詳細: {traceback.format_exc()}")

    def update_last_update_date(self, date_str=None):
        """最終更新日を更新して保存"""
        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")

        self.last_update_date = date_str
        self.save()
        self.logger.debug(f"最終更新日を更新しました: {date_str}")

    def __str__(self):
        """設定の文字列表現を返す"""
        return (
            f"Config(db_path={self.db_path}, "
            f"log_level={self.log_level}, "
            f"auto_update={self.auto_update}, "
            f"update_winticket={self.update_winticket}, "
            f"update_yenjoy={self.update_yenjoy})"
        )
