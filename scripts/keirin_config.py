"""
競輪データ取得システムの設定を管理するモジュール
"""

import configparser
import logging
import os
import threading
from datetime import datetime, timedelta

# ロギング設定
logger = logging.getLogger("KeirinConfig")


def find_config_file(config_file=None):
    """
    設定ファイルを検索する

    以下の順序で検索し、最初に見つかったファイルのパスを返す:
    1. 指定されたパス (config_file引数)
    2. スクリプトの親ディレクトリの config/config.ini (プロジェクトルート)
    3. カレントディレクトリの config.ini (非推奨)
    4. スクリプトと同じディレクトリの config.ini (非推奨)
    5. ユーザーのホームディレクトリの .keirin/config.ini (非推奨)

    Args:
        config_file (str, optional): 指定された設定ファイルパス

    Returns:
        str: 見つかった設定ファイルのパス、または指定されたデフォルトパス
    """
    if config_file and os.path.exists(config_file):
        return os.path.abspath(config_file)

    # 検索する場所のリスト (config/config.ini を優先)
    search_paths = [
        # プロジェクトルートの config/config.ini
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "config",
            "config.ini",
        ),
        # 以下、古いパス (互換性のため残すが非推奨)
        os.path.join(os.getcwd(), "config.ini"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini"),
        os.path.join(os.path.expanduser("~"), ".keirin", "config.ini"),
    ]

    for path in search_paths:
        if os.path.exists(path):
            return os.path.abspath(path)

    # 見つからない場合はデフォルトパス (プロジェクトルート/config/config.ini)
    default_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "config",
        "config.ini",
    )
    logger.warning(
        f"設定ファイルが見つかりませんでした。デフォルトパスを使用します: {default_path}"
    )
    return default_path


class KeirinConfig:
    """
    競輪データ取得システムの設定を管理するクラス (古いバージョンの可能性あり)

    iniファイルから設定を読み込み、プログラム全体で共有する
    """

    # クラス変数として正しいファイル名を定義
    CONFIG_FILENAME = "config.ini"
    CONFIG_DIR = "config"

    def __init__(self, config_file=None, logger=None):
        """
        初期化

        Args:
            config_file (str, optional): 設定ファイルのパス
            logger (logging.Logger, optional): ロガーオブジェクト
        """
        self.logger = logger or logging.getLogger(__name__)
        # find_config_file を使ってパスを決定
        self.config_file = find_config_file(config_file)
        self.config = configparser.ConfigParser()

        try:
            if os.path.exists(self.config_file):
                self.config.read(self.config_file, encoding="utf-8")
                self.logger.info(f"設定ファイルを読み込みました: {self.config_file}")
            else:
                self.logger.warning(f"設定ファイルが見つかりません: {self.config_file}")
                # デフォルト設定を作成 (正しいパスで)
                self._create_default_config(self.config_file)
        except Exception as e:
            self.logger.error(f"設定ファイルの読み込み中にエラーが発生しました: {e}")
            # デフォルト設定を作成 (正しいパスで)
            self._create_default_config(self.config_file)

    def _get_project_root(self):
        """プロジェクトのルートディレクトリを取得"""
        # scripts ディレクトリの親ディレクトリをルートとする
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def _get_default_config_path(self):
        """デフォルトの設定ファイルパスを取得"""
        return os.path.join(
            self._get_project_root(), self.CONFIG_DIR, self.CONFIG_FILENAME
        )

    def _create_default_config(self, filepath=None):
        """デフォルト設定を作成する"""
        target_path = filepath or self._get_default_config_path()
        self.config = configparser.ConfigParser()
        project_root = self._get_project_root()

        # [daemon] セクション
        self.config["daemon"] = {
            "db_path": "MySQL Database (Connection managed by KeirinDataAccessor)",  # MySQL移行
            "update_interval": "1",  # 時間
            "check_interval": "30",  # 分
            "retry_interval": "30",  # 分
        }

        # [update] セクション
        self.config["update"] = {
            "update_winticket": "true",
            "update_yenjoy": "true",
            "update_interval": "2",  # 秒
            "days_to_update": "30",
            "get_all_date": "false",
            "all_start_date": "20230101",
            "start_date": self._get_date_string(30),  # 30日前
            "end_date": self._get_date_string(0),  # 今日
        }

        # [log] セクション
        self.config["log"] = {
            "log_level": "INFO",
            "log_dir": os.path.join(project_root, "logs"),  # Logパス修正
        }

        # ファイルに書き込み
        try:
            # ディレクトリが存在しない場合は作成
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with open(target_path, "w", encoding="utf-8") as f:
                self.config.write(f)
            self.logger.info(f"デフォルト設定ファイルを作成しました: {target_path}")
        except Exception as e:
            self.logger.error(f"設定ファイルの書き込み中にエラーが発生しました: {e}")

    def _write_default_config(self, file):
        # このメソッドは _create_default_config に統合されたため、不要になる可能性あり
        # 必要であれば _create_default_config のロジックを参考に修正
        self.logger.warning(
            "_write_default_config は非推奨です。_create_default_config を使用してください。"
        )
        pass  # 実装は省略

    def _get_date_string(self, days_ago=0):
        """
        指定日数前の日付文字列を取得

        Args:
            days_ago (int): 何日前か

        Returns:
            str: 日付文字列（YYYYMMDD）
        """
        target_date = datetime.now() - timedelta(days=days_ago)
        return target_date.strftime("%Y%m%d")

    def get_daemon_config(self):
        """
        デーモン設定を取得

        Returns:
            dict: デーモン設定
        """
        defaults = {
            "db_path": "MySQL Database (Connection managed by KeirinDataAccessor)",
            "update_interval": 1,
            "check_interval": 30,
            "retry_interval": 30,
        }

        if "daemon" not in self.config:
            return defaults

        return {
            "db_path": self.config.get(
                "daemon", "db_path", fallback=defaults["db_path"]
            ),
            "update_interval": self.config.getint(
                "daemon", "update_interval", fallback=defaults["update_interval"]
            ),
            "check_interval": self.config.getint(
                "daemon", "check_interval", fallback=defaults["check_interval"]
            ),
            "retry_interval": self.config.getint(
                "daemon", "retry_interval", fallback=defaults["retry_interval"]
            ),
        }

    def get_update_config(self):
        """
        更新設定を取得

        Returns:
            dict: 更新設定
        """
        defaults = {
            "update_winticket": True,
            "update_yenjoy": True,
            "update_interval": 2,
            "days_to_update": 30,
            "get_all_date": False,
            "all_start_date": "20230101",
            "start_date": self._get_date_string(30),
            "end_date": self._get_date_string(0),
        }

        if "update" not in self.config:
            return defaults

        return {
            "update_winticket": self.config.getboolean(
                "update", "update_winticket", fallback=defaults["update_winticket"]
            ),
            "update_yenjoy": self.config.getboolean(
                "update", "update_yenjoy", fallback=defaults["update_yenjoy"]
            ),
            "update_interval": self.config.getint(
                "update", "update_interval", fallback=defaults["update_interval"]
            ),
            "days_to_update": self.config.getint(
                "update", "days_to_update", fallback=defaults["days_to_update"]
            ),
            "get_all_date": self.config.getboolean(
                "update", "get_all_date", fallback=defaults["get_all_date"]
            ),
            "all_start_date": self.config.get(
                "update", "all_start_date", fallback=defaults["all_start_date"]
            ),
            "start_date": self.config.get(
                "update", "start_date", fallback=defaults["start_date"]
            ),
            "end_date": self.config.get(
                "update", "end_date", fallback=defaults["end_date"]
            ),
        }

    def get_log_config(self):
        """
        ログ設定を取得

        Returns:
            dict: ログ設定
        """
        defaults = {
            "log_level": "INFO",
            "log_dir": os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs"
            ),
        }

        if "log" not in self.config:
            return defaults

        return {
            "log_level": self.config.get(
                "log", "log_level", fallback=defaults["log_level"]
            ),
            "log_dir": self.config.get("log", "log_dir", fallback=defaults["log_dir"]),
        }

    def save(self):
        """設定をファイルに保存"""
        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                self.config.write(f)
            self.logger.info(f"設定を保存しました: {self.config_file}")
            return True
        except Exception as e:
            self.logger.error(f"設定の保存中にエラーが発生しました: {e}")
            return False


# グローバルアクセス用関数
_config_instance = None


def get_config(config_file=None):
    """
    設定インスタンスを取得 (KeirinConfig)

    Args:
        config_file (str, optional): 設定ファイルのパス

    Returns:
        KeirinConfig: 設定インスタンス
    """
    global _config_instance
    if _config_instance is None or (
        config_file
        and _config_instance.config_file != get_current_config_file_path(config_file)
    ):
        _config_instance = KeirinConfig(config_file)
    return _config_instance


def get_current_config_file_path():
    """
    設定ファイルを探す

    Returns:
        str: 設定ファイルのパス
    """
    return get_config().config_file


# シングルトン実装を改善（スレッドセーフに）
class KeirinConfigSingleton:
    """スレッドセーフなシングルトン実装"""

    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, config_file=None):
        """
        シングルトンインスタンスを取得

        Args:
            config_file (str, optional): 設定ファイルのパス

        Returns:
            KeirinConfig: 設定インスタンス
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = KeirinConfig(config_file)
            return cls._instance

    @classmethod
    def reset_instance(cls):
        """
        シングルトンインスタンスをリセット（主にテスト用）
        """
        with cls._lock:
            cls._instance = None


# 使用例
if __name__ == "__main__":
    # ロギング設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # 設定の読み込み
    config = get_config()

    # 設定の表示
    print("デーモン設定:", config.get_daemon_config())
    print("更新設定:", config.get_update_config())
    print("ログ設定:", config.get_log_config())

    # 設定の変更
    config.config["daemon"]["update_interval"] = "4"
    print("変更後のデーモン設定:", config.get_daemon_config())

    # 設定の保存
    config.save()
