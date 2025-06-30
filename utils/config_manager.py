import configparser
import logging
import os
from typing import Any, Optional

CONFIG_FILE_NAME = "config.ini"  # 設定ファイル名


class ConfigManager:
    """
    config.ini ファイルを読み込み、設定値へのアクセスを提供するシングルトンクラス。
    """

    _instance = None
    _config = None
    _logger = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._config = configparser.ConfigParser()
            cls._logger = logging.getLogger(__name__)
            cls._load_config()  # インスタンス生成時に読み込み
        return cls._instance

    @classmethod
    def _load_config(cls):
        """設定ファイルを読み込む"""
        config_path = cls._get_config_path()
        if not os.path.exists(config_path):
            cls._logger.error(f"設定ファイルが見つかりません: {config_path}")
            # ここでデフォルト設定を生成するか、エラーを発生させるか選択
            # cls._create_default_config(config_path) # 例: デフォルト生成
            return False

        try:
            cls._logger.info(f"設定ファイルを読み込みます: {config_path}")
            # encoding='utf-8' を指定 (iniファイルがUTF-8の場合)
            cls._config.read(config_path, encoding="utf-8")
            return True
        except configparser.Error as e:
            cls._logger.error(
                f"設定ファイルの読み込み中にエラーが発生しました: {e}", exc_info=True
            )
            return False
        except Exception as e:
            cls._logger.error(
                f"設定ファイルの読み込み中に予期せぬエラー: {e}", exc_info=True
            )
            return False

    @classmethod
    def _get_config_path(cls) -> str:
        """設定ファイルの絶対パスを取得"""
        # アプリケーションのルートディレクトリを取得 (utils フォルダの親)
        app_root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(app_root_dir, "config", CONFIG_FILE_NAME)

    # --- 設定値取得メソッド ---

    def get_value(
        self, section: str, key: str, fallback: Optional[Any] = None
    ) -> Optional[str]:
        """指定されたセクションとキーの値を取得 (文字列)"""
        if not self._config:
            return fallback
        return self._config.get(section, key, fallback=fallback)

    def get_boolean(self, section: str, key: str, fallback: bool = False) -> bool:
        """指定されたセクションとキーの値をブール値として取得"""
        if not self._config:
            return fallback
        try:
            # getboolean は 'yes', 'true', '1', 'on' などを True として解釈
            return self._config.getboolean(section, key, fallback=fallback)
        except ValueError:
            self._logger.warning(
                f"設定値 [{section}] {key} はブール値として解釈できません。フォールバック値 '{fallback}' を使用します。"
            )
            return fallback
        except configparser.NoSectionError:
            # self._logger.debug(f"設定セクション [{section}] が見つかりません。フォールバック値 '{fallback}' を使用します。")
            return fallback
        except configparser.NoOptionError:
            # self._logger.debug(f"設定キー [{section}] {key} が見つかりません。フォールバック値 '{fallback}' を使用します。")
            return fallback

    def get_int(
        self, section: str, key: str, fallback: Optional[int] = None
    ) -> Optional[int]:
        """指定されたセクションとキーの値を整数として取得"""
        if not self._config:
            return fallback
        try:
            return self._config.getint(section, key, fallback=fallback)
        except ValueError:
            self._logger.warning(
                f"設定値 [{section}] {key} は整数として解釈できません。フォールバック値 '{fallback}' を使用します。"
            )
            return fallback
        except configparser.NoSectionError:
            return fallback
        except configparser.NoOptionError:
            return fallback

    def get_float(
        self, section: str, key: str, fallback: Optional[float] = None
    ) -> Optional[float]:
        """指定されたセクションとキーの値を浮動小数点数として取得"""
        if not self._config:
            return fallback
        try:
            return self._config.getfloat(section, key, fallback=fallback)
        except ValueError:
            self._logger.warning(
                f"設定値 [{section}] {key} は浮動小数点数として解釈できません。フォールバック値 '{fallback}' を使用します。"
            )
            return fallback
        except configparser.NoSectionError:
            return fallback
        except configparser.NoOptionError:
            return fallback

    # --- 設定値の動的な更新 (UIからの保存用) ---
    # 注意: アプリケーション実行中に設定を変更する場合、関連するオブジェクトの再初期化が必要になる可能性がある

    def set_value(self, section: str, key: str, value: Any):
        """設定値を更新 (メモリ上)"""
        if not self._config:
            return
        if not self._config.has_section(section):
            self._config.add_section(section)
        self._config.set(section, key, str(value))  # 値は文字列として保存
        self._logger.info(f"設定値を更新しました: [{section}] {key} = {value}")

    def save_config(self):
        """現在の設定をファイルに保存"""
        if not self._config:
            return False
        config_path = self._get_config_path()
        try:
            with open(config_path, "w", encoding="utf-8") as configfile:
                self._config.write(configfile)
            self._logger.info(f"設定をファイルに保存しました: {config_path}")
            return True
        except IOError as e:
            self._logger.error(
                f"設定ファイルの保存中にエラーが発生しました: {e}", exc_info=True
            )
            return False
        except Exception as e:
            self._logger.error(
                f"設定ファイルの保存中に予期せぬエラー: {e}", exc_info=True
            )
            return False

    # --- スケジュール設定 (JSON形式) ---
    def get_schedule_list(self, fallback: Optional[list] = None) -> list:
        """スケジュール設定をリストとして取得 (JSONからデコード)"""
        if fallback is None:
            fallback = []
        json_str = self.get_value("Schedule", "schedule_list", fallback="[]")
        try:
            import json

            schedule_list = json.loads(json_str)
            if not isinstance(schedule_list, list):
                self._logger.warning(
                    "[Schedule] schedule_list がリスト形式ではありません。デフォルト([])を返します。"
                )
                return fallback
            # TODO: 各要素の形式チェック (time, steps, enabled が存在するか等) も入れるとより堅牢
            return schedule_list
        except json.JSONDecodeError:
            self._logger.error(
                f"[Schedule] schedule_list のJSONデコードに失敗しました。デフォルト([])を返します。: {json_str[:100]}..."
            )
            return fallback
        except Exception as e:
            self._logger.error(
                f"スケジュールリストの取得中に予期せぬエラー: {e}", exc_info=True
            )
            return fallback

    def set_schedule_list(self, schedule_list: list):
        """スケジュールリストを設定 (JSON形式で保存)"""
        try:
            import json

            # インデントなしでコンパクトに保存
            json_str = json.dumps(
                schedule_list, ensure_ascii=False, separators=(",", ":")
            )
            self.set_value("Schedule", "schedule_list", json_str)
            self._logger.info(
                f"スケジュールリストを更新しました (JSON): {json_str[:100]}..."
            )
        except TypeError as e:
            self._logger.error(
                f"スケジュールリストのJSONエンコードに失敗しました: {e}", exc_info=True
            )
        except Exception as e:
            self._logger.error(
                f"スケジュールリストの設定中に予期せぬエラー: {e}", exc_info=True
            )

    # --- デフォルト設定生成 (オプション) ---
    # def _create_default_config(cls, config_path):
    #     cls._logger.info(f"デフォルト設定ファイルを生成します: {config_path}")
    #     cls._config['DATABASE'] = {'db_path': 'MySQL Database'}
    #     cls._config['UPDATE'] = {
    #         'update_winticket': 'True',
    #         'update_yenjoy': 'True',
    #         'fetch_cups': 'True',
    #         'fetch_cup_details': 'True',
    #         'fetch_race_data': 'True',
    #         'fetch_odds_data': 'True',
    #         'fetch_yenjoy_results': 'True',
    #         'auto_update': 'False',
    #         'auto_update_interval': '60'
    #     }
    #     try:
    #         with open(config_path, 'w', encoding='utf-8') as configfile:
    #             cls._config.write(configfile)
    #     except IOError as e:
    #         cls._logger.error(f"デフォルト設定ファイルの生成に失敗しました: {e}")


# --- シングルトンインスタンスの取得関数 ---
def get_config_manager() -> ConfigManager:
    """ConfigManager のシングルトンインスタンスを取得"""
    return ConfigManager()
