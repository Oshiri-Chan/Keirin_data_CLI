"""
データ更新サービス
"""

import logging
from typing import List, Optional, Tuple, Dict, Any

# from database.keirin_database import KeirinDatabase # KeirinDatabase は直接使わないのでコメントアウトまたは削除
# from utils.logger_manager import LoggerManager # 削除: LoggerManager は使用しない
# from utils.time_utils import get_current_datetime_string # 削除: 未使用のため
import threading
from datetime import datetime

from api.winticket_api import WinticketAPI  # 修正: services.api -> api
from api.yenjoy_api import YenjoyAPI  # 修正: services.api -> api
from database.db_accessor import KeirinDataAccessor  # ★ KeirinDataAccessor をインポート

# Extractor のインポートを追加
from database.extractors.extract_data_for_step2 import Step2DataExtractor
from database.extractors.extract_data_for_step3 import Step3DataExtractor
from database.extractors.extract_data_for_step4 import Step4DataExtractor

# 各ステップごとのSaverをインポート
from services.savers.step1_saver import Step1Saver
from services.savers.step2_saver import Step2Saver
from services.savers.step3_saver import Step3Saver
from services.savers.step4_saver import Step4Saver
from services.savers.step5_saver import Step5Saver

# 分割したステップアップデーターをインポート
from services.updaters import (
    Step1Updater,
    Step2Updater,
    Step3Updater,
    Step4Updater,
    Step5Updater,
)
from utils.config_manager import ConfigManager, get_config_manager

# サービスの初期化時にデータセーバーを初期化
# from services.data_saver import DataSaver # コメントアウト
# from services.winticket_data_saver import WinticketDataSaver # コメントアウト
# from services.yenjoy_data_saver import YenjoyDataSaver # コメントアウト


class UpdateService:
    """
    データの更新を担当するクラス
    """

    def __init__(
        self,
        winticket_api: WinticketAPI,
        yenjoy_api: YenjoyAPI,
        # db_instance: KeirinDatabase, # KeirinDatabase の代わりに KeirinDataAccessor を受け取る
        db_accessor: KeirinDataAccessor,  # ★ 引数名を db_accessor に変更
        logger: logging.Logger,  # 修正: logger_manager: LoggerManager -> logger
        config_manager: ConfigManager,
        default_max_workers: int = 5,
        winticket_rate_limit_wait: float = 1.0,
        yenjoy_rate_limit_wait_html: float = 2.0,
        yenjoy_rate_limit_wait_api: float = 1.0,
    ):
        """
        初期化

        Args:
            winticket_api: WinticketAPIインスタンス
            yenjoy_api: YenjoyAPIインスタンス
            db_accessor: KeirinDataAccessorインスタンス
            logger: logging.Loggerインスタンス # 修正
            config_manager: ConfigManagerインスタンス
            default_max_workers: デフォルトの最大並列処理数
            winticket_rate_limit_wait: WinticketAPIのレートリミット待機時間
            yenjoy_rate_limit_wait_html: YenjoyAPIのレートリミット待機時間
            yenjoy_rate_limit_wait_api: YenjoyAPIのレートリミット待機時間
        """
        self.winticket_api = winticket_api
        self.yenjoy_api = yenjoy_api
        # self.db = db_instance # ★ KeirinDatabase の保持をやめる
        self.db_accessor = db_accessor  # ★ KeirinDataAccessor を保持
        self.logger = logger  # 修正: logger_manager.get_logger(__name__) -> logger

        # YenjoyAPIにrate_limit設定を適用 (loggerを設定した後に実行)
        if hasattr(self.yenjoy_api, "request_interval"):
            self.yenjoy_api.request_interval = yenjoy_rate_limit_wait_html
            self.logger.info(
                f"YenjoyAPI request_interval を {yenjoy_rate_limit_wait_html}秒 に設定しました"
            )
        self.config_manager = config_manager

        # ★★★ self.config と関連設定を早期に初期化 ★★★
        self.config = self.config_manager or get_config_manager()
        self.default_max_workers = self.config.get_int(
            "PERFORMANCE", "max_workers", fallback=5
        )
        self.saver_batch_size = self.config.get_int(
            "PERFORMANCE", "saver_batch_size", fallback=50
        )
        # default_rate_limit_winticket と default_rate_limit_yenjoy も必要であればインスタンス変数としてここで設定
        # self.default_rate_limit_winticket = self.config.get_float('PERFORMANCE', 'rate_limit_winticket', fallback=0.1)
        # self.default_rate_limit_yenjoy = self.config.get_float('PERFORMANCE', 'rate_limit_yenjoy', fallback=1.0)
        # ★★★ 初期化ここまで ★★★

        # 各ステップごとのSaverインスタンスをここで明示的に作成
        # KeirinDataAccessor を渡すように修正
        # db_accessor = self.db.accessor # KeirinDatabaseがaccessorプロパティを持つと仮定 ← この行は不要になる

        self.step1_saver = Step1Saver(
            self.db_accessor, self.logger
        )  # ★ self.db_accessor を使用
        self.step2_saver = Step2Saver(
            self.db_accessor, self.logger
        )  # ★ self.db_accessor を使用
        self.step3_saver = Step3Saver(
            self.db_accessor, self.logger
        )  # ★ self.db_accessor を使用
        self.step4_saver = Step4Saver(
            self.db_accessor, self.logger
        )  # ★ self.db_accessor を使用
        self.step5_saver = Step5Saver(
            self.db_accessor, self.logger
        )  # ★ self.db_accessor を使用

        # Updaterインスタンスの作成
        # 各Updaterが必要とする引数に合わせて修正
        self.step1_updater = Step1Updater(
            api_client=self.winticket_api,
            saver=self.step1_saver,  # saver を渡す
            # db_accessor=self.db_accessor, # Step1Updaterのコンストラクタにないため削除
            logger=self.logger,
            # rate_limit_wait=winticket_rate_limit_wait # Step1Updaterのコンストラクタにないため削除
        )
        self.step2_updater = Step2Updater(
            api_client=self.winticket_api,
            saver=self.step2_saver,  # saver を渡す
            # db_accessor=self.db_accessor, # Step2Updaterのコンストラクタにないため削除
            logger=self.logger,
            # rate_limit_wait=winticket_rate_limit_wait # Step2Updaterのコンストラクタにないため削除
        )
        self.step3_updater = Step3Updater(
            api_client=self.winticket_api,  # APIクライアントを渡す
            saver=self.step3_saver,
            logger=self.logger,
            max_workers=self.config.get_int(
                "PERFORMANCE", "step3_max_workers", fallback=1
            ),
            rate_limit_wait=self.config.get_float(
                "PERFORMANCE", "rate_limit_winticket", fallback=1.0
            ),
        )
        self.step4_updater = Step4Updater(
            api_client=self.winticket_api,  # yenjoy_api から winticket_api に変更
            step4_saver=self.step4_saver,
            logger=self.logger,
            max_workers=self.default_max_workers,  # max_workers を渡す
            rate_limit_wait=self.config.get_float(
                "PERFORMANCE", "rate_limit_winticket", fallback=1.0
            ),  # Yenjoy用からWinticket用に適切なレートリミットに変更 (設定ファイルから取得する例)
        )
        self.step5_updater = Step5Updater(
            api_client=self.yenjoy_api,
            step5_saver=self.step5_saver,
            db_accessor=self.db_accessor,  # db_accessor を渡す (Step5Updaterは期待している)
            logger=self.logger,
            max_workers=self.default_max_workers,  # max_workers を渡す
            rate_limit_wait_html=yenjoy_rate_limit_wait_html,  # rate_limit_wait_html を渡す
        )

        self.cancel_event = threading.Event()
        self.logger.info("UpdateService initialized.")

        # Extractor の初期化を追加
        self.step2_extractor = Step2DataExtractor(self.db_accessor, self.logger)
        self.step3_extractor = Step3DataExtractor(self.db_accessor, self.logger)
        self.step4_extractor = Step4DataExtractor(self.db_accessor, self.logger)

        # 設定値を取得して使用 # ★★★ このブロックは上記で統合したので削除またはコメントアウト ★★★
        # self.config = config_manager or get_config_manager()
        # self.default_max_workers = self.config.get_int('PERFORMANCE', 'max_workers', fallback=5) # インスタンス変数として保存
        # default_rate_limit_winticket = self.config.get_float('PERFORMANCE', 'rate_limit_winticket', fallback=0.1)
        # default_rate_limit_yenjoy = self.config.get_float('PERFORMANCE', 'rate_limit_yenjoy', fallback=1.0)

    def update_period_step_by_step(
        self,
        start_date_str: str,
        end_date_str: str,
        steps: List[str],
        venue_codes: Optional[List[str]] = None,
        specific_race_ids: Optional[List[str]] = None,
        force_update_all: bool = False,
    ) -> Tuple[bool, Dict[str, Dict[str, Any]]]:
        """
        指定期間のデータを段階的に更新する

        Args:
            start_date_str (str): 開始日（YYYY-MM-DD）
            end_date_str (str): 終了日（YYYY-MM-DD）
            steps (list of str): 実行するステップのリスト（1〜5の数字、または'step1'〜'step5'の文字列）
                                  Noneの場合はすべてのステップを実行
            venue_codes (list of str, optional): 特定の開催IDを指定する場合
            specific_race_ids (list of str, optional): 特定のレースIDを指定する場合
            force_update_all (bool, optional): 更新ステータスを無視して強制的に更新するかどうか. Defaults to False.
            callback (callable, optional): 進捗通知コールバック関数
                                           callback(step_name, current_step, total_steps, message)

        Returns:
            tuple: (成功したかどうか, 結果辞書)
                結果辞書の形式: {
                    'steps': {
                        'step1': {'success': bool, 'message': str, 'data': any},
                        'step2': {'success': bool, 'message': str, 'data': any},
                        ...
                    },
                    'total_success': bool,
                    'messages': list of str,
                    'error': str or None
                }
        """
        self.logger.info(
            f"期間 {start_date_str} から {end_date_str} までのデータを段階的に更新します。ステップ: {steps}, 開催ID: {venue_codes}, 強制更新: {force_update_all}"
        )

        if steps is None:
            steps = [1, 2, 3, 4, 5]

        normalized_steps = []
        for step in steps:
            if isinstance(step, int) and 1 <= step <= 5:
                normalized_steps.append(step)
            elif isinstance(step, str) and step.startswith("step") and len(step) > 4:
                try:
                    step_num = int(step[4:])
                    if 1 <= step_num <= 5:
                        normalized_steps.append(step_num)
                except ValueError:
                    self.logger.warning(f"無効なステップ指定: {step}")
            else:
                self.logger.warning(f"無効なステップ指定: {step}")

        if not normalized_steps:
            self.logger.error("有効なステップが指定されていません")
            return False, {
                "error": "有効なステップが指定されていません",
                "steps": {},
                "total_success": False,
                "messages": [],
            }

        results = {"steps": {}, "total_success": True, "messages": [], "error": None}
        critical_steps = {1, 2, 5}

        # 各ステップの処理
        for step_num in normalized_steps:
            if self.cancel_event.is_set():
                self.logger.info("処理がキャンセルされました。")
                results["messages"].append("処理がキャンセルされました。")
                results["total_success"] = False
                break

            step_name = f"step{step_num}"
            step_success = False
            step_message = ""
            step_data_count = 0

            try:
                self.logger.info(f"--- {step_name} を開始します ---")
                if step_num == 1:
                    step_success, step_message, step_data_count = self._update_step1(
                        start_date_str,
                        end_date_str,
                        venue_codes=venue_codes,
                        force_update_all=force_update_all,
                    )
                elif step_num == 2:
                    step_success, step_message, step_data_count = self._update_step2(
                        start_date_str,
                        end_date_str,
                        venue_codes=venue_codes,
                        force_update_all=force_update_all,
                    )
                elif step_num == 3:
                    step_success, step_message, step_data_count = self._update_step3(
                        start_date_str,
                        end_date_str,
                        venue_codes=venue_codes,
                        force_update_all=force_update_all,
                    )
                elif step_num == 4:
                    step_success, step_message, step_data_count = self._update_step4(
                        start_date_str,
                        end_date_str,
                        venue_codes=venue_codes,
                        force_update_all=force_update_all,
                    )
                elif step_num == 5:
                    step_success, step_message, step_data_count = self._update_step5(
                        start_date_str,
                        end_date_str,
                        venue_codes=venue_codes,
                        specific_race_ids=specific_race_ids,
                        force_update_all=force_update_all,
                    )

                self.logger.info(
                    f"--- {step_name} が完了しました ({'成功' if step_success else '失敗'}) ---"
                )

            except Exception as e:
                step_message = f"{step_name} 処理中に予期せぬエラー: {e}"
                self.logger.error(step_message, exc_info=True)
                step_success = False

            results["steps"][step_name] = {
                "success": step_success,
                "message": step_message,
                "count": step_data_count,
            }
            results["messages"].append(f"{step_name}: {step_message}")
            if not step_success and step_num in critical_steps:
                self.logger.warning(
                    f"重要なステップ {step_name} が失敗したため、以降のステップを中止します。"
                )
                results["total_success"] = False
                results["error"] = f"重要なステップ {step_name} が失敗しました。"
                break
            if not step_success:
                results["total_success"] = False

        return results["total_success"], results

    def _update_step1(
        self,
        start_date: str,
        end_date: str,
        venue_codes: Optional[List[str]] = None,
        force_update_all: bool = False,
    ):
        """ステップ1: 開催日程情報をWinticket APIから取得・保存"""
        self.logger.info(
            f"Step1: 開催日程情報の更新を開始 (期間: {start_date} - {end_date}, 会場: {venue_codes}, 強制: {force_update_all})"
        )
        try:
            # Step1 updater doesn't currently support force_update_all parameter
            # It processes venue_codes differently than other steps
            if venue_codes:
                self.logger.warning(
                    "Step1: venue_codes filtering not implemented in update_period. Processing full period."
                )
            success, result_data = self.step1_updater.update_period(
                start_date, end_date
            )
            updated_count = (
                len(result_data.get("cups", [])) if isinstance(result_data, dict) else 0
            )
            msg = f"開催日程情報 {updated_count} 件を更新しました。"
            self.logger.info(f"Step1完了: {msg}")
            return True, msg, updated_count
        except Exception as e:
            self.logger.error(f"Step1処理中にエラー: {e}", exc_info=True)
            return False, f"Step1エラー: {e}", 0

    def _update_step2(
        self,
        start_date: str,
        end_date: str,
        venue_codes: Optional[List[str]] = None,
        force_update_all: bool = False,
    ):
        """ステップ2: レース基本情報・番組情報をWinticket APIから取得・保存"""
        self.logger.info(
            f"Step2: レース基本情報の更新を開始 (期間: {start_date} - {end_date}, 会場: {venue_codes}, 強制: {force_update_all})"
        )
        try:
            extracted_data = self.step2_extractor.extract(
                start_date=start_date,
                end_date=end_date,
                venue_codes=venue_codes,
                force_update_all=force_update_all,
            )
            cup_ids_to_update = extracted_data.get("cup_ids_for_update", [])
            if not cup_ids_to_update:
                msg = "Step2: 更新対象の開催情報が見つかりませんでした。"
                self.logger.info(msg)
                return True, msg, 0
            self.logger.info(
                f"Step2: {len(cup_ids_to_update)} 件の開催情報を取得します。"
            )
            success, result_info = self.step2_updater.update_cups(cup_ids_to_update)
            saved_races = result_info.get("saved_races", 0)
            saved_schedules = result_info.get("saved_schedules", 0)
            msg = f"開催情報: スケジュール {saved_schedules} 件、レース {saved_races} 件を保存。"
            self.logger.info(f"Step2完了: {msg}")
            return success, msg, saved_races + saved_schedules
        except Exception as e:
            self.logger.error(f"Step2処理中にエラー: {e}", exc_info=True)
            return False, f"Step2エラー: {e}", 0

    def _update_step3(
        self,
        start_date: str,
        end_date: str,
        venue_codes: Optional[List[str]] = None,
        force_update_all: bool = False,
    ):
        """ステップ3: レース詳細情報（出走表、選手コメント）をWinticket APIから取得・保存"""
        self.logger.info(
            f"Step3: レース詳細情報の更新を開始 (期間: {start_date} - {end_date}, 会場: {venue_codes}, 強制: {force_update_all})"
        )
        total_updated_count = 0
        all_success = True
        error_messages = []
        try:
            target_items_for_extraction = []
            id_type = None
            if venue_codes:
                target_items_for_extraction = venue_codes
                id_type = "cup_id"
            elif start_date and end_date:
                target_items_for_extraction = [(start_date, end_date)]
                id_type = "period"
            else:
                self.logger.warning(
                    "Step3: 開催IDまたは期間が指定されていません。スキップします。"
                )
                return True, "開催IDまたは期間が指定されていないためスキップ", 0
            for item in target_items_for_extraction:
                if self.cancel_event.is_set():
                    break
                races_for_update_step3 = []
                if id_type == "cup_id":
                    extracted_data_step3 = self.step3_extractor.extract(
                        cup_id=item, force_update_all=force_update_all
                    )
                    races_for_update_step3 = extracted_data_step3.get(
                        "races_for_update", []
                    )
                elif id_type == "period":
                    extracted_data_step3 = self.step3_extractor.extract(
                        start_date=item[0],
                        end_date=item[1],
                        force_update_all=force_update_all,
                    )
                    races_for_update_step3 = extracted_data_step3.get(
                        "races_for_update", []
                    )
                if not races_for_update_step3:
                    self.logger.info(
                        f"Step3: 開催ID/期間 {item} に更新対象のレース詳細情報が見つかりませんでした。"
                    )
                    continue
                self.logger.info(
                    f"Step3: 開催ID/期間 {item} で {len(races_for_update_step3)} 件のレース詳細情報を更新します。"
                )
                success, result_info = self.step3_updater.update_races_step3(
                    races_for_update_step3,
                    batch_size=50,
                    with_parallel=True,
                    force_update=force_update_all,
                )
                # 結果を従来の形式に変換
                success_count_updater = result_info.get("succeeded_saves", 0)
                error_count_updater = result_info.get("failed_saves", 0)
                total_updated_count += success_count_updater
                if error_count_updater > 0:
                    all_success = False
                    error_messages.append(
                        f"開催ID/期間 {item} で {error_count_updater}件のエラー発生。"
                    )
            msg = f"レース詳細情報 {total_updated_count} 件を更新しました。"
            if not all_success:
                msg += " いくつかのエラーが発生しました: " + "; ".join(error_messages)
            self.logger.info(f"Step3完了: {msg}")
            return all_success, msg, total_updated_count
        except Exception as e:
            self.logger.error(f"Step3処理中にエラー: {e}", exc_info=True)
            return False, f"Step3エラー: {e}", 0

    def _update_step4(
        self,
        start_date: str,
        end_date: str,
        venue_codes: Optional[List[str]] = None,
        force_update_all: bool = False,
    ):
        """ステップ4: オッズ情報をWinticket APIから取得・保存"""
        self.logger.info(
            f"Step4: オッズ情報の更新を開始 (期間: {start_date} - {end_date}, 会場: {venue_codes}, 強制: {force_update_all})"
        )
        total_updated_count = 0
        all_success = True
        error_messages = []
        try:
            target_items_for_extraction = []
            id_type = None
            if venue_codes:
                target_items_for_extraction = venue_codes
                id_type = "cup_id"
            elif start_date and end_date:
                target_items_for_extraction = [(start_date, end_date)]
                id_type = "period"
            else:
                self.logger.warning(
                    "Step4: 開催IDまたは期間が指定されていません。スキップします。"
                )
                return True, "開催IDまたは期間が指定されていないためスキップ", 0
            for item in target_items_for_extraction:
                if self.cancel_event.is_set():
                    break
                races_for_odds_update = []
                if id_type == "cup_id":
                    races_for_odds_update = self.step4_extractor.extract(
                        cup_id=item, force_update_all=force_update_all
                    )
                elif id_type == "period":
                    races_for_odds_update = self.step4_extractor.extract(
                        start_date=item[0],
                        end_date=item[1],
                        force_update_all=force_update_all,
                    )
                if not races_for_odds_update:
                    self.logger.info(
                        f"Step4: 開催ID/期間 {item} に更新対象のオッズ情報が見つかりませんでした。"
                    )
                    continue
                self.logger.info(
                    f"Step4: 開催ID/期間 {item} で {len(races_for_odds_update)} 件のオッズ情報を更新します。"
                )
                success, result_info = self.step4_updater.update_odds_bulk(
                    races_for_odds_update,
                    batch_size=50,
                    with_parallel=True,
                    force_update_all=force_update_all,
                )
                # 結果を従来の形式に変換
                success_count_updater = result_info.get("successful_saves", 0)
                error_count_updater = result_info.get("failed_saves", 0)
                total_updated_count += success_count_updater
                if error_count_updater > 0:
                    all_success = False
                    error_messages.append(
                        f"開催ID/期間 {item} で {error_count_updater}件のエラー発生。"
                    )
            msg = f"オッズ情報 {total_updated_count} 件を更新しました。"
            if not all_success:
                msg += " いくつかのエラーが発生しました: " + "; ".join(error_messages)
            self.logger.info(f"Step4完了: {msg}")
            return all_success, msg, total_updated_count
        except Exception as e:
            self.logger.error(f"Step4処理中にエラー: {e}", exc_info=True)
            return False, f"Step4エラー: {e}", 0

    def _update_step5(
        self,
        start_date: str,
        end_date: str,
        venue_codes: Optional[List[str]] = None,
        specific_race_ids: Optional[List[str]] = None,
        force_update_all: bool = False,
    ):
        """ステップ5: HTMLパース結果（レース結果、周回、コメント等）をYenJoy APIから取得・保存"""
        self.logger.info(
            f"Step5: HTMLパース結果の更新を開始 (期間: {start_date} - {end_date}, 会場: {venue_codes}, 特定レース: {specific_race_ids}, 強制: {force_update_all})"
        )
        try:
            # Step5Updaterのupdate_results_bulkメソッドが内部でレース情報取得も行うため、
            # 直接そのメソッドを呼び出す（パラメータから対象レースを決定）
            if specific_race_ids:
                self.logger.warning(
                    "Step5: specific_race_ids指定は現在サポートされていません。期間ベースでの処理を行います。"
                )
                if not start_date or not end_date:
                    from datetime import datetime, timedelta

                    today = datetime.now()
                    start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
                    end_date = today.strftime("%Y-%m-%d")

            if not start_date or not end_date:
                self.logger.warning("Step5: 期間が指定されていません。スキップします。")
                return True, "更新対象の期間が指定されていないためスキップ", 0

            # Step5Updaterのupdate_results_bulkメソッドを直接使用
            result = self.step5_updater.update_results_bulk(
                start_date, end_date, venue_codes, force_update_all
            )

            if result.get("success"):
                # resultにprocessed_countなどが含まれている場合はそれを使用、なければ0
                processed_count = result.get("processed_count", 0)
                msg = f"HTMLパース結果 {processed_count} 件を更新しました。"
                if result.get("details"):
                    # 詳細情報があれば追加
                    details = result["details"]
                    if details.get("failed_count", 0) > 0:
                        msg += f" ({details.get('failed_count', 0)}件のエラーあり)"
                self.logger.info(f"Step5完了: {msg}")
                return True, msg, processed_count
            else:
                msg = f"Step5エラー: {result.get('message', '不明なエラー')}"
                self.logger.error(msg)
                return False, msg, 0
        except Exception as e:
            self.logger.error(f"Step5処理中にエラー: {e}", exc_info=True)
            return False, f"Step5エラー: {e}", 0

    def update_cup(self, cup_id: str, steps: List[str], force_update_all: bool = False):
        """
        指定されたカップIDに基づいてデータを更新する

        Args:
            cup_id (str): 更新対象のカップID
            steps (list of str): 更新するステップのリスト
            force_update_all (bool, optional): 全てのデータを更新するかどうか. Defaults to False.

        Returns:
            tuple: (成功したかどうか, メッセージ)
        """
        self.logger.info(
            f"開催ID {cup_id} のデータを更新します。ステップ: {steps}, 強制更新: {force_update_all}"
        )
        cup_info = self.db_accessor.get_cup_info(cup_id)
        if (
            not cup_info
            or not cup_info.get("start_date")
            or not cup_info.get("end_date")
        ):
            self.logger.error(
                f"開催ID {cup_id} の情報が見つからないか、日付情報が不完全です。処理を中止します。"
            )
            return False, {
                "error": f"開催ID {cup_id} の情報取得失敗",
                "steps": {},
                "total_success": False,
                "messages": [],
            }
        start_date_dt = cup_info["start_date"]
        end_date_dt = cup_info["end_date"]
        start_date_str_cup = (
            start_date_dt.strftime("%Y-%m-%d")
            if isinstance(start_date_dt, datetime)
            else str(start_date_dt)
        )
        end_date_str_cup = (
            end_date_dt.strftime("%Y-%m-%d")
            if isinstance(end_date_dt, datetime)
            else str(end_date_dt)
        )
        return self.update_period_step_by_step(
            start_date_str=start_date_str_cup,
            end_date_str=end_date_str_cup,
            steps=steps,
            venue_codes=[cup_id],
            force_update_all=force_update_all,
        )

    def cleanup_connections(self):
        """
        データベース接続を安全にクリーンアップする

        このメソッドは更新処理の終了時に呼び出すべきです。

        Returns:
            bool: クリーンアップが成功したかどうか
        """
        try:
            # データベースインスタンスが存在し、safe_cleanupメソッドを持っている場合は呼び出す
            if (
                hasattr(self, "db_accessor")
                and self.db_accessor
                and hasattr(self.db_accessor, "safe_cleanup")
            ):
                self.logger.info("データベース接続の安全なクリーンアップを実行します")
                self.db_accessor.safe_cleanup()
                return True
            # close_connectionメソッドを持っている場合は呼び出す
            elif (
                hasattr(self, "db_accessor")
                and self.db_accessor
                and hasattr(self.db_accessor, "close_connection")
            ):
                self.logger.info("データベース接続を閉じます")
                self.db_accessor.close_connection()
                return True
            else:
                self.logger.warning(
                    "データベースインスタンスが見つからないか、クリーンアップメソッドがありません"
                )
                return False
        except Exception as e:
            self.logger.error(
                f"データベース接続のクリーンアップ中にエラーが発生しました: {str(e)}"
            )
            return False

    # 他のメソッドは既存のものをそのまま保持
    # ...
