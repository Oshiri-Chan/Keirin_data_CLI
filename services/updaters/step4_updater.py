"""
ステップ4: オッズ情報の取得・更新クラス
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

from api.winticket_api import WinticketAPI  # WinticketAPI をインポート

# from api.yenjoy_api import YenjoyAPI # F401: imported but unused
from services.savers.step4_saver import Step4Saver

# from database.db_accessor import KeirinDataAccessor # KeirinDataAccessor は未使用のため削除

# --- バッチサイズの定義 (Step3と同じものを使うか、別途定義) ---
RACE_BATCH_SIZE = 100  # 一度に処理するレース数

# 終了済みとみなすレースステータス (races.status の値)。実際の値に合わせてください。
# 例: FINISHED_RACE_STATUSES = {'finished', 'canceled', 'payout_completed', 'completed_by_system'}
FINISHED_RACE_STATUSES: Set[str] = {"3"}  # レース終了のサインが "3" であることを反映


class Step4Updater:
    """
    ステップ4: オッズ情報をAPIから取得し、Saverに渡すクラス
    """

    def __init__(
        self,
        api_client: WinticketAPI,
        step4_saver: Step4Saver,
        logger: Optional[logging.Logger] = None,
        max_workers: int = 3,
        rate_limit_wait: float = 1.0,
    ):
        """
        初期化
        Args:
            api_client: YenJoy APIクライアントのインスタンス
            step4_saver: Step4Saverのインスタンス
            logger: ロガーインスタンス
            max_workers: API呼び出しの並列処理時の最大ワーカー数
            rate_limit_wait: 順次処理時のAPI呼び出し間隔 (秒)
        """
        self.api_client = api_client
        self.saver = step4_saver  # Step4Saverのインスタンスを保持
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self.rate_limit_wait = rate_limit_wait

        # オッズデータ変換用の設定（Saverから移動）
        self.odds_table_configs = {
            # 2車単 (Exacta)
            "exacta": {
                "table_name": "odds_exacta",
                "api_data_key": "exacta",
                "api_combination_key": "numbers",
                "db_combination_col": "key",
                "api_main_odds_key": "odds",
                "db_main_odds_col": "odds",
                "api_min_odds_key": "minOdds",
                "db_min_odds_col": "min_odds",
                "api_max_odds_key": "maxOdds",
                "db_max_odds_col": "max_odds",
                "additional_cols_mapping": {
                    "type": "type",
                    "popularity_order": "popularityOrder",
                    "odds_str": "oddsStr",
                    "min_odds_str": "minOddsStr",
                    "max_odds_str": "maxOddsStr",
                    "unit_price": "unitPrice",
                    "payoff_unit_price": "payoffUnitPrice",
                    "absent": "absent",
                },
            },
            # 2車複 (Quinella)
            "quinella": {
                "table_name": "odds_quinella",
                "api_data_key": "quinella",
                "api_combination_key": "numbers",
                "db_combination_col": "key",
                "api_main_odds_key": "odds",
                "db_main_odds_col": "odds",
                "api_min_odds_key": "minOdds",
                "db_min_odds_col": "min_odds",
                "api_max_odds_key": "maxOdds",
                "db_max_odds_col": "max_odds",
                "additional_cols_mapping": {
                    "type": "type",
                    "popularity_order": "popularityOrder",
                    "odds_str": "oddsStr",
                    "min_odds_str": "minOddsStr",
                    "max_odds_str": "maxOddsStr",
                    "unit_price": "unitPrice",
                    "payoff_unit_price": "payoffUnitPrice",
                    "absent": "absent",
                },
            },
            # ワイド (Quinella Place)
            "quinellaPlace": {
                "table_name": "odds_quinella_place",
                "api_data_key": "quinellaPlace",
                "api_combination_key": "numbers",
                "db_combination_col": "key",
                "api_main_odds_key": "odds",
                "db_main_odds_col": "odds",
                "api_min_odds_key": "minOdds",
                "db_min_odds_col": "min_odds",
                "api_max_odds_key": "maxOdds",
                "db_max_odds_col": "max_odds",
                "additional_cols_mapping": {
                    "type": "type",
                    "popularity_order": "popularityOrder",
                    "odds_str": "oddsStr",
                    "min_odds_str": "minOddsStr",
                    "max_odds_str": "maxOddsStr",
                    "unit_price": "unitPrice",
                    "payoff_unit_price": "payoffUnitPrice",
                    "absent": "absent",
                },
            },
            # 3連単 (Trifecta)
            "trifecta": {
                "table_name": "odds_trifecta",
                "api_data_key": "trifecta",
                "api_combination_key": "numbers",
                "db_combination_col": "key",
                "api_main_odds_key": "odds",
                "db_main_odds_col": "odds",
                "api_min_odds_key": "minOdds",
                "db_min_odds_col": "min_odds",
                "api_max_odds_key": "maxOdds",
                "db_max_odds_col": "max_odds",
                "additional_cols_mapping": {
                    "type": "type",
                    "popularity_order": "popularityOrder",
                    "odds_str": "oddsStr",
                    "min_odds_str": "minOddsStr",
                    "max_odds_str": "maxOddsStr",
                    "unit_price": "unitPrice",
                    "payoff_unit_price": "payoffUnitPrice",
                    "absent": "absent",
                },
            },
            # 3連複 (Trio)
            "trio": {
                "table_name": "odds_trio",
                "api_data_key": "trio",
                "api_combination_key": "numbers",
                "db_combination_col": "key",
                "api_main_odds_key": "odds",
                "db_main_odds_col": "odds",
                "api_min_odds_key": "minOdds",
                "db_min_odds_col": "min_odds",
                "api_max_odds_key": "maxOdds",
                "db_max_odds_col": "max_odds",
                "additional_cols_mapping": {
                    "type": "type",
                    "popularity_order": "popularityOrder",
                    "odds_str": "oddsStr",
                    "min_odds_str": "minOddsStr",
                    "max_odds_str": "maxOddsStr",
                    "unit_price": "unitPrice",
                    "payoff_unit_price": "payoffUnitPrice",
                    "absent": "absent",
                },
            },
            # 枠単 (Bracket Exacta)
            "bracketExacta": {
                "table_name": "odds_bracket_exacta",
                "api_data_key": "bracketExacta",
                "api_combination_key": "brackets",
                "db_combination_col": "key",
                "api_main_odds_key": "odds",
                "db_main_odds_col": "odds",
                "api_min_odds_key": "minOdds",
                "db_min_odds_col": "min_odds",
                "api_max_odds_key": "maxOdds",
                "db_max_odds_col": "max_odds",
                "additional_cols_mapping": {
                    "type": "type",
                    "popularity_order": "popularityOrder",
                    "odds_str": "oddsStr",
                    "min_odds_str": "minOddsStr",
                    "max_odds_str": "maxOddsStr",
                    "unit_price": "unitPrice",
                    "payoff_unit_price": "payoffUnitPrice",
                    "absent": "absent",
                },
            },
            # 枠複 (Bracket Quinella)
            "bracketQuinella": {
                "table_name": "odds_bracket_quinella",
                "api_data_key": "bracketQuinella",
                "api_combination_key": "brackets",
                "db_combination_col": "key",
                "api_main_odds_key": "odds",
                "db_main_odds_col": "odds",
                "api_min_odds_key": "minOdds",
                "db_min_odds_col": "min_odds",
                "api_max_odds_key": "maxOdds",
                "db_max_odds_col": "max_odds",
                "additional_cols_mapping": {
                    "type": "type",
                    "popularity_order": "popularityOrder",
                    "odds_str": "oddsStr",
                    "min_odds_str": "minOddsStr",
                    "max_odds_str": "maxOddsStr",
                    "unit_price": "unitPrice",
                    "payoff_unit_price": "payoffUnitPrice",
                    "absent": "absent",
                },
            },
        }

    def _fetch_odds_info_worker(
        self, race_identifier: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]], bool]:
        """
        単一レースのオッズ情報をAPIから取得するワーカー関数
        Returns:
            Tuple[Optional[str], Optional[Dict[str, Any]], bool]: (race_id, fetched_data, is_api_call_successful)
            fetched_dataがNoneでis_api_call_successfulがTrueの場合、APIは成功したがデータがなかったことを示す
        """
        race_id = race_identifier.get("race_id")
        cup_id = race_identifier.get("cup_id")
        race_number = race_identifier.get("number")
        index = race_identifier.get("race_index")  # 日付インデックス (day)
        thread_id = threading.current_thread().ident

        if not all([race_id, cup_id, index is not None, race_number is not None]):
            self.logger.error(
                f"スレッド {thread_id}: [Step4 Updater] レース識別情報またはindexが不完全です: {race_identifier}"
            )
            return race_id, None, False  # API呼び出し以前のエラー

        try:
            self.logger.debug(
                f"スレッド {thread_id}: [Step4 Updater] レースID {race_id} (Cup: {cup_id}, Day: {index}, No: {race_number}) のオッズ情報を取得開始"
            )
            odds_data = self.api_client.get_odds_data(cup_id, index, race_number)

            if not odds_data:
                self.logger.warning(
                    f"スレッド {thread_id}: [Step4 Updater] レースID {race_id} のオッズ情報がAPIから取得できませんでした (レスポンスが空)。"
                )
                # API呼び出しは成功したがデータがないケース
                return race_id, None, True

            # --- データの実質的な空判定 ---
            odds_data["race_id"] = race_id  # Saver で利用するため race_id を付与
            is_effectively_empty = True
            if hasattr(self.saver, "odds_table_configs"):
                # Step4Saverのconfigを参照して、いずれかのオッズ種別データが存在するか確認
                for config_key in self.saver.odds_table_configs.keys():
                    api_key = self.saver.odds_table_configs[config_key][
                        "api_data_key"
                    ]  # APIレスポンス内のキー名
                    if api_key in odds_data and odds_data[api_key]:
                        # データがリスト形式の場合、空リストでないかも確認
                        if isinstance(odds_data[api_key], list) and odds_data[api_key]:
                            is_effectively_empty = False
                            break
                        # データがリスト形式でない場合 (例: オッズステータス情報など)、存在すればOKとするケースも考慮？
                        # 現在の実装ではリストのみチェックしている
                else:  # for ループに対応する else
                    # Saverにconfigがない場合、判定をスキップして空ではないとみなす（警告ログ）
                    self.logger.warning(
                        f"スレッド {thread_id}: [Step4 Updater] Race ID {race_id}: self.saver.odds_table_configs が見つかりません。オッズデータの空判定をスキップします。"
                    )
                    is_effectively_empty = False
            # if hasattr のブロックはここで終了 (インデント調整で明確化)

            if is_effectively_empty:
                self.logger.info(
                    f"スレッド {thread_id}: [Step4 Updater] Race ID {race_id} のオッズデータは実質的に空です。"
                )
                # API呼び出しは成功したが、有効なデータが含まれていないケース
                # この場合、fetched_data として race_id のみを含む辞書などを返しても良いが、
                # Saver側での処理を考えると、データがないことを示すために None を返す方がシンプルかもしれない。
                # ステータス更新のために API 成功フラグは True のままにする。
                return race_id, None, True  # API成功、データ空
            # --- 空判定ここまで ---

            self.logger.debug(
                f"スレッド {thread_id}: [Step4 Updater] レースID {race_id} のオッズ情報を取得完了"
            )
            return race_id, odds_data, True  # API成功、データ有り

        except Exception as e:
            # API呼び出し中の例外発生
            self.logger.error(
                f"スレッド {thread_id}: [Step4 Worker ERROR] レースID {race_id} のオッズ情報取得中にエラー: {type(e).__name__} - {e}",
                exc_info=True,
            )
            return race_id, None, False  # API呼び出し失敗

    def update_odds_bulk(
        self,
        races_to_update: List[Dict[str, Any]],
        batch_size: int,
        with_parallel: bool = True,
        force_update_all: bool = False,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        指定されたレースリストのオッズ情報をAPIから取得し、バッチ処理で保存し、ステータスを更新する

        Step4の更新方針：
        - レース未終了（races.status!='3'）：step4_statusに関係なく常に上書き更新、ステータスは'completed'にしない
        - レース終了済み（races.status='3'）：過去に更新履歴があれば上書き更新、更新後に'completed'に設定
        - 強制更新モード：すべてのレースを更新対象とする

        Args:
            races_to_update (List[Dict[str, Any]]): 更新対象のレース情報リスト
            batch_size (int): Saverに渡すDB保存時のバッチサイズ
            with_parallel (bool): APIリクエストを並列実行するかどうか
            force_update_all (bool): 強制更新モード。Trueの場合、ステータス'3'（完了済み）のレースも更新対象に含める
        Returns:
            Tuple[bool, Dict[str, Any]]: (成功フラグ, 結果詳細辞書)
        """
        if not races_to_update:
            self.logger.warning("[Step4 Updater] 更新対象レースがありません。")
            return False, {
                "message": "No races provided",
                "processed_races": 0,
                "successful_fetches": 0,
                "successful_saves": 0,
                "skipped_empty": 0,
                "failed_fetches": 0,
                "failed_saves": 0,
                "skipped_finished": 0,
            }

        thread_id = threading.current_thread().ident
        total_races_input = len(races_to_update)
        self.logger.info(
            f"スレッド {thread_id}: [Step4 Updater] 開始 (入力レース数: {total_races_input}, バッチサイズ: {RACE_BATCH_SIZE})"
        )

        all_odds_data_to_save: List[Dict[str, Any]] = []

        race_ids_processed: Set[str] = set()
        race_ids_api_success_data: Set[str] = set()
        race_ids_api_success_no_data: Set[str] = set()
        race_ids_api_failed: Set[str] = set()
        race_ids_skipped_finished: Set[str] = (
            set()
        )  # 終了済みのためスキップしたレースID

        # --- 処理対象のレースをフィルタリング ---
        # レースステータスと過去の更新履歴を取得
        race_ids_to_check = [r["race_id"] for r in races_to_update if "race_id" in r]
        current_race_statuses: Dict[str, str] = {}
        odds_update_history: Dict[str, bool] = {}  # race_id -> 過去に更新履歴があるか

        if race_ids_to_check:
            try:
                # レースステータスを取得
                current_race_statuses = self.saver.get_race_statuses(race_ids_to_check)
                # 過去のオッズ更新履歴を確認（odds_statusesテーブルをチェック）
                odds_update_history = self.saver.check_odds_update_history(
                    race_ids_to_check
                )
            except AttributeError as ae:
                self.logger.error(
                    f"[Step4 Updater] self.saver に必要なメソッドが存在しません: {ae}"
                )
            except Exception as e:
                self.logger.error(
                    f"[Step4 Updater] レースステータス・更新履歴の取得中にエラー: {e}",
                    exc_info=True,
                )

        active_races_to_process: List[Dict[str, Any]] = []
        for race_info in races_to_update:
            race_id = race_info.get("race_id")
            if not race_id:
                self.logger.warning(
                    f"[Step4 Updater] race_id のないレース情報が含まれています: {race_info}"
                )
                continue

            race_ids_processed.add(
                race_id
            )  # 入力されたものは全て処理対象の母数としてカウント

            current_status = current_race_statuses.get(race_id)
            has_update_history = odds_update_history.get(race_id, False)

            # 強制更新モードの場合はステータスチェックをバイパス
            if force_update_all:
                self.logger.info(
                    f"[Step4 Updater] レースID {race_id}: 強制更新モードのため、ステータス '{current_status}' に関係なく処理対象に含めます。"
                )
                active_races_to_process.append(race_info)
            elif current_status and current_status in FINISHED_RACE_STATUSES:
                # レース終了済みの場合、過去に更新履歴があれば処理対象とする
                if has_update_history:
                    self.logger.info(
                        f"[Step4 Updater] レースID {race_id}: レース終了済み (ステータス '{current_status}') ですが、過去に更新履歴があるため最終更新を実行します。"
                    )
                    active_races_to_process.append(race_info)
                else:
                    self.logger.info(
                        f"[Step4 Updater] レースID {race_id}: レース終了済み (ステータス '{current_status}') かつ更新履歴なしのため、スキップします。"
                    )
                    race_ids_skipped_finished.add(race_id)
            else:
                # レースが未終了の場合は、step4_statusに関係なく上書き更新
                if not current_status:
                    self.logger.warning(
                        f"[Step4 Updater] レースID {race_id} の現在ステータスが取得できませんでした。処理を続行します。"
                    )
                else:
                    self.logger.info(
                        f"[Step4 Updater] レースID {race_id}: レース未終了 (ステータス '{current_status}') のため、オッズデータを上書き更新します。"
                    )
                active_races_to_process.append(race_info)
        # --- フィルタリングここまで ---

        total_races_to_fetch_api = len(active_races_to_process)
        self.logger.info(
            f"[Step4 Updater] API取得対象レース数: {total_races_to_fetch_api} (終了済みスキップ: {len(race_ids_skipped_finished)})"
        )

        for i in range(0, total_races_to_fetch_api, RACE_BATCH_SIZE):
            current_batch_races = active_races_to_process[i : i + RACE_BATCH_SIZE]
            batch_num = i // RACE_BATCH_SIZE + 1
            total_batches = (
                (total_races_to_fetch_api + RACE_BATCH_SIZE - 1) // RACE_BATCH_SIZE
                if total_races_to_fetch_api > 0
                else 0
            )
            self.logger.info(
                f"--- [Step4 Updater] バッチ {batch_num}/{total_batches} API取得開始 ({len(current_batch_races)}件) ---"
            )

            batch_race_ids_to_try = {
                r["race_id"] for r in current_batch_races if "race_id" in r
            }
            # race_ids_processed は既に全入力レースIDを保持しているので、ここでは更新不要

            if batch_race_ids_to_try:
                # --- ステータスを 'processing' に更新 ---
                try:
                    self.saver.update_race_step4_status_batch(
                        list(batch_race_ids_to_try), "processing"
                    )
                    self.logger.info(
                        f"[Step4 Updater] バッチ {batch_num}: {len(batch_race_ids_to_try)} 件を 'processing' に更新。"
                    )
                except Exception as e:
                    self.logger.error(
                        f"[Step4 Updater] バッチ {batch_num}: 'processing' へのステータス更新中にエラー: {e}",
                        exc_info=True,
                    )
                    # 更新失敗しても処理は続行するが、失敗したIDは記録しておく
                    race_ids_api_failed.update(
                        batch_race_ids_to_try
                    )  # API実行前に失敗とみなす
                    continue  # このバッチのAPI取得はスキップ
                # --- 更新ここまで ---

                futures = {}
                # --- 並列/順次 実行 (変更なし) ---
                if (
                    with_parallel
                    and len(current_batch_races) > 1
                    and self.max_workers > 1
                ):
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        for race_info in current_batch_races:
                            # 'processing' 更新失敗でスキップされたレースは除外
                            if race_info.get("race_id") not in race_ids_api_failed:
                                futures[
                                    executor.submit(
                                        self._fetch_odds_info_worker, race_info
                                    )
                                ] = race_info.get("race_id")
                else:
                    for race_info in current_batch_races:
                        if race_info.get("race_id") not in race_ids_api_failed:

                            class ImmediateFuture:
                                def __init__(self, result_val, race_id_val):
                                    self._result = result_val
                                    self._race_id = race_id_val

                                def result(self):
                                    return self._result

                                def __hash__(self):
                                    return id(self)

                                def __eq__(self, other):
                                    return id(self) == id(other)

                            result_tuple = self._fetch_odds_info_worker(race_info)
                            future_obj = ImmediateFuture(
                                result_tuple, race_info.get("race_id")
                            )
                            futures[future_obj] = race_info.get("race_id")
                            if (
                                self.rate_limit_wait > 0
                                and len(current_batch_races) > 1
                            ):
                                time.sleep(self.rate_limit_wait)
                # --- 実行ここまで ---

                # --- 結果の集計 ---
                completed_futures_iterator = (
                    as_completed(futures.keys())
                    if (
                        with_parallel
                        and len(current_batch_races) > 1
                        and self.max_workers > 1
                    )
                    else futures.keys()
                )

                for future_key_obj in completed_futures_iterator:
                    race_id_from_future = futures[future_key_obj]
                    # すでに失敗リストに入っているものはスキップ
                    if race_id_from_future in race_ids_api_failed:
                        continue
                    try:
                        actual_future_object = future_key_obj
                        # _fetch_odds_info_worker の戻り値 (race_id, fetched_data, is_api_call_successful) を受け取る
                        _, fetched_data, is_api_call_successful = (
                            actual_future_object.result()
                        )

                        if is_api_call_successful:
                            if fetched_data:
                                # API成功 & データ有り
                                all_odds_data_to_save.append(fetched_data)
                                race_ids_api_success_data.add(race_id_from_future)
                            else:
                                # API成功 & データ空
                                self.logger.info(
                                    f"[Step4 Updater] バッチ {batch_num}: Race ID {race_id_from_future} はAPI成功、データ空。"
                                )
                                race_ids_api_success_no_data.add(race_id_from_future)
                        else:
                            # API失敗
                            self.logger.warning(
                                f"[Step4 Updater] バッチ {batch_num}: Race ID {race_id_from_future} のAPI取得失敗。"
                            )
                            race_ids_api_failed.add(race_id_from_future)
                    except Exception as e:
                        # Futureの結果取得自体でのエラー
                        self.logger.error(
                            f"[Step4 Updater] バッチ {batch_num}: Race ID {race_id_from_future} のFuture結果取得中エラー: {e}",
                            exc_info=True,
                        )
                        race_ids_api_failed.add(race_id_from_future)
                # --- 集計ここまで ---

                self.logger.info(
                    f"--- [Step4 Updater] バッチ {batch_num}/{total_batches} API取得完了 ---"
                )

        # --- 全バッチ終了後の処理 ---
        self.logger.info(
            f"[Step4 Updater] 全API取得完了。処理対象(入力): {len(race_ids_processed)}, "
            f"API成功(データ有): {len(race_ids_api_success_data)}, "
            f"API成功(データ空): {len(race_ids_api_success_no_data)}, "
            f"API失敗: {len(race_ids_api_failed)}, "
            f"終了済みスキップ: {len(race_ids_skipped_finished)}"
        )

        # --- データ保存処理 ---
        successful_save_ids: Set[str] = set()
        failed_save_ids: Set[str] = set()

        if all_odds_data_to_save:
            self.logger.info(
                f"[Step4 Updater] {len(all_odds_data_to_save)}件の取得済みオッズデータを一括保存開始..."
            )
            try:
                # Step4Saver.save_all_odds_for_race をレースごとに呼び出す
                for race_odds_data in all_odds_data_to_save:
                    race_id = race_odds_data.get("race_id")
                    if not race_id:
                        self.logger.warning(
                            f"[Step4 Updater] 保存データに race_id がないためスキップ: {race_odds_data.keys()}"
                        )
                        continue

                    # API成功リストに含まれているか確認 (念のため、通常は含まれているはず)
                    if race_id not in race_ids_api_success_data:
                        self.logger.warning(
                            f"[Step4 Updater] Race ID {race_id} はAPI成功リストにないため保存スキップ（データ不整合の可能性）。"
                        )
                        failed_save_ids.add(race_id)  # 保存失敗として扱う
                        continue

                    try:
                        # APIレスポンスをSaver用の形式に変換
                        transformed_data = self._transform_odds_api_response(
                            race_id, race_odds_data
                        )
                        save_success = self.saver.save_all_odds_for_race(
                            race_id, transformed_data, batch_size
                        )
                        if save_success:
                            successful_save_ids.add(race_id)
                    except Exception as e_race_save:
                        self.logger.error(
                            f"[Step4 Updater] Race ID {race_id} のオッズ保存中に個別エラー: {e_race_save}",
                            exc_info=True,
                        )
                        failed_save_ids.add(race_id)

                # API成功したが保存に失敗したIDを計算 (successful_save_ids を使って)
                api_success_but_save_failed = (
                    race_ids_api_success_data - successful_save_ids
                )
                failed_save_ids.update(
                    api_success_but_save_failed
                )  # failed_save_ids にマージ

                self.logger.info(
                    f"[Step4 Updater] オッズデータの一括保存完了。成功: {len(successful_save_ids)}, 保存失敗(API取得は成功): {len(api_success_but_save_failed)}"
                )

            except Exception as e:
                self.logger.error(
                    f"[Step4 Updater] オッズデータの一括保存処理全体でエラー: {e}",
                    exc_info=True,
                )
                # 保存処理全体が失敗した場合、API成功したものはすべて保存失敗とみなす
                failed_save_ids.update(race_ids_api_success_data)
        else:
            self.logger.info("[Step4 Updater] 保存対象のオッズデータがありません。")

        # --- 最終ステータス更新 ---
        self.logger.info("[Step4 Updater] 最終ステータス更新開始...")

        # レース終了済みの場合のみ 'completed' にする
        # 1. 終了済みでスキップしたもの（過去に更新履歴なし）
        # 2. 終了済みで今回更新したもの（過去に更新履歴あり）
        ids_to_mark_completed = set()
        ids_to_mark_completed.update(race_ids_skipped_finished)  # 終了済みスキップ

        # 保存成功したレースのうち、レース終了済みのもののみをcompletedにする
        for race_id in successful_save_ids:
            race_status = current_race_statuses.get(race_id, "")
            if race_status in FINISHED_RACE_STATUSES:
                ids_to_mark_completed.add(race_id)
                self.logger.debug(
                    f"[Step4 Updater] レースID {race_id}: 終了済み（ステータス '{race_status}'）のためcompletedに設定します。"
                )
            else:
                self.logger.debug(
                    f"[Step4 Updater] レースID {race_id}: 未終了（ステータス '{race_status}'）のためcompletedに設定しません。"
                )

        if ids_to_mark_completed:
            try:
                self.saver.update_race_step4_status_batch(
                    list(ids_to_mark_completed), "completed"
                )
                self.logger.info(
                    f"[Step4 Updater] {len(ids_to_mark_completed)} 件のレース終了済みレースを 'completed' に更新。"
                )
            except Exception as e:
                self.logger.error(
                    f"[Step4 Updater] 'completed' ステータス更新中にエラー: {e}",
                    exc_info=True,
                )
                failed_save_ids.update(
                    ids_to_mark_completed
                )  # 更新失敗したものは failed 扱いにする

        # API成功したがデータがなかったものを 'no_data' に (ただし、completed や failed リストに含まれていないもの)
        ids_to_mark_no_data = list(
            race_ids_api_success_no_data
            - ids_to_mark_completed
            - failed_save_ids
            - race_ids_api_failed
        )
        if ids_to_mark_no_data:
            try:
                self.saver.update_race_step4_status_batch(
                    ids_to_mark_no_data, "no_data"
                )
                self.logger.info(
                    f"[Step4 Updater] {len(ids_to_mark_no_data)} 件のレースを 'no_data' に更新。"
                )
            except Exception as e:
                self.logger.error(
                    f"[Step4 Updater] 'no_data' ステータス更新中にエラー: {e}",
                    exc_info=True,
                )
                # 失敗したIDは次の 'failed' 更新で拾われるようにする
                failed_save_ids.update(
                    ids_to_mark_no_data
                )  # ここでは failed_save_ids に追加する

        # API失敗 または 保存失敗したものを 'failed' に (ただし、既に completed になっていないもの)
        combined_failed_or_save_failed = race_ids_api_failed.union(failed_save_ids)
        ids_to_mark_failed = list(
            combined_failed_or_save_failed - ids_to_mark_completed
        )  # completed は除外
        if ids_to_mark_failed:
            try:
                self.saver.update_race_step4_status_batch(ids_to_mark_failed, "failed")
                # ログ内訳
                failed_api_in_marked = race_ids_api_failed.intersection(
                    set(ids_to_mark_failed)
                )
                failed_save_in_marked = failed_save_ids.intersection(
                    set(ids_to_mark_failed)
                )
                self.logger.info(
                    f"[Step4 Updater] {len(ids_to_mark_failed)} 件のレースを 'failed' に更新。 (内訳 API失敗: {len(failed_api_in_marked)}, 保存失敗: {len(failed_save_in_marked)})"
                )
            except Exception as e:
                self.logger.error(
                    f"[Step4 Updater] 'failed' ステータス更新中にエラー: {e}",
                    exc_info=True,
                )

        self.logger.info("[Step4 Updater] 最終ステータス更新完了。")

        # --- 結果サマリ ---
        overall_success = (
            len(successful_save_ids) > 0 or len(race_ids_skipped_finished) > 0
        )  # 1件でも保存成功またはスキップ(完了扱い)があればTrue

        result_summary = {
            "message": "Step4 update process completed.",
            "total_races_input": total_races_input,  # total_races_to_process から変更
            "tried_api_fetches": len(race_ids_processed)
            - len(race_ids_skipped_finished),  # 実際にAPIを試行した数
            "skipped_as_finished": len(race_ids_skipped_finished),
            "successful_api_fetches_with_data": len(race_ids_api_success_data),
            "successful_api_fetches_empty_data": len(race_ids_api_success_no_data),
            "failed_api_fetches": len(race_ids_api_failed),
            "successful_saves": len(successful_save_ids),
            "failed_saves_after_fetch_success": len(
                race_ids_api_success_data - successful_save_ids
            ),
            "final_completed_count": len(
                ids_to_mark_completed
            ),  # 実際にcompletedになった数
            "final_no_data_count": len(ids_to_mark_no_data),
            "final_failed_count": len(ids_to_mark_failed),
        }
        self.logger.info(f"[Step4 Updater] 完了。結果: {result_summary}")

        return overall_success, result_summary

    def _prepare_odds_data_for_batch(
        self, race_id: str, odds_api_list: List[Dict[str, Any]], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        特定のオッズ種別のAPIデータをDB保存用に整形する
        """
        to_save_list = []
        if not odds_api_list:
            return to_save_list

        for item_api_data in odds_api_list:
            if not isinstance(item_api_data, dict):
                continue

            db_row: Dict[str, Any] = {"race_id": race_id}

            # 組み合わせキーの処理
            if config.get("api_combination_key") and config.get("db_combination_col"):
                # APIでは実際には"key"フィールドが使われる
                combination_data = item_api_data.get("key")
                if combination_data is None:
                    # フォールバック：設定上のキー名でも試行
                    combination_data = item_api_data.get(config["api_combination_key"])

                if not isinstance(combination_data, list) or not all(
                    isinstance(x, (str, int)) for x in combination_data
                ):
                    self.logger.warning(
                        f"Race {race_id}, Table {config['table_name']}: 組み合わせキー(key) が不正: {combination_data}"
                    )
                    continue

                sep = "-"
                # Quinella, Trio, QuinellaPlaceはソート
                sort_target_tables = [
                    "odds_quinella",
                    "odds_trio",
                    "odds_quinella_place",
                ]
                if config["table_name"] in sort_target_tables:
                    try:
                        combination_key_str = sep.join(
                            sorted(map(str, combination_data), key=int)
                        )
                    except ValueError:
                        combination_key_str = sep.join(
                            sorted(map(str, combination_data))
                        )
                else:
                    combination_key_str = sep.join(map(str, combination_data))

                db_row[config["db_combination_col"]] = combination_key_str

            # 主要オッズの処理
            if config.get("api_main_odds_key") and config.get("db_main_odds_col"):
                odds_val = item_api_data.get(config["api_main_odds_key"])
                db_row[config["db_main_odds_col"]] = (
                    float(odds_val) if odds_val is not None else None
                )

            # 最小オッズの処理
            if config.get("api_min_odds_key") and config.get("db_min_odds_col"):
                min_odds_val = item_api_data.get(config["api_min_odds_key"])
                db_row[config["db_min_odds_col"]] = (
                    float(min_odds_val) if min_odds_val is not None else None
                )

            # 最大オッズの処理
            if config.get("api_max_odds_key") and config.get("db_max_odds_col"):
                max_odds_val = item_api_data.get(config["api_max_odds_key"])
                db_row[config["db_max_odds_col"]] = (
                    float(max_odds_val) if max_odds_val is not None else None
                )

            # 追加カラムの処理
            if "additional_cols_mapping" in config:
                for db_col, api_col in config["additional_cols_mapping"].items():
                    val = item_api_data.get(api_col)

                    # typeフィールドの特別処理：NOT NULLなので必須
                    if db_col == "type":
                        if val is not None:
                            try:
                                db_row[db_col] = int(val)
                            except (ValueError, TypeError):
                                self.logger.warning(
                                    f"Race {race_id}, Table {config['table_name']}: type値 '{val}' をintに変換できません。デフォルト値 6 を使用します。"
                                )
                                db_row[db_col] = 6  # デフォルト値
                        else:
                            # APIからtypeが取得できない場合、オッズタイプに応じてデフォルト値を設定
                            default_type_map = {
                                "exacta": 6,  # 2車単
                                "quinella": 7,  # 2車複
                                "quinellaPlace": 5,  # ワイド
                                "trifecta": 8,  # 3連単
                                "trio": 9,  # 3連複
                                "bracketExacta": 1,  # 枠単
                                "bracketQuinella": 2,  # 枠複
                            }
                            odds_type_key = config.get("api_data_key", "exacta")
                            db_row[db_col] = default_type_map.get(odds_type_key, 6)
                            self.logger.info(
                                f"Race {race_id}, Table {config['table_name']}: typeフィールドがAPIにないため、デフォルト値 {db_row[db_col]} を使用します。"
                            )
                    # 型変換
                    elif db_col == "absent":
                        db_row[db_col] = bool(val) if val is not None else None
                    elif val is not None:
                        try:
                            if isinstance(val, (int, float)):
                                db_row[db_col] = val
                            elif (
                                isinstance(val, str)
                                and any(c.isdigit() for c in val)
                                and "." in val
                            ):
                                db_row[db_col] = float(val)
                            elif isinstance(val, str) and val.isdigit():
                                db_row[db_col] = int(val)
                            else:
                                db_row[db_col] = val
                        except ValueError:
                            self.logger.warning(
                                f"Race {race_id}, Table {config['table_name']}, Column {db_col}: 値 '{val}' の型変換に失敗しました。"
                            )
                            db_row[db_col] = val
                    else:
                        db_row[db_col] = None

            to_save_list.append(db_row)
        return to_save_list

    def _transform_odds_api_response(
        self, race_id: str, odds_api_response: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        APIレスポンスをSaver用の整形済みデータに変換する
        """
        transformed_data: Dict[str, Any] = {"race_id": race_id}

        # 各オッズ種別のデータを変換
        for config_key, config in self.odds_table_configs.items():
            api_data_key = config["api_data_key"]
            if api_data_key in odds_api_response:
                odds_list = odds_api_response[api_data_key]
                if isinstance(odds_list, list) and odds_list:
                    # APIデータをDB用に変換
                    formatted_odds = self._prepare_odds_data_for_batch(
                        race_id, odds_list, config
                    )
                    transformed_data[config_key] = formatted_odds
                else:
                    transformed_data[config_key] = []
            else:
                transformed_data[config_key] = []

        # オッズステータス情報の変換
        odds_status = {
            "race_id": race_id,
            "trifecta_payoff_status": odds_api_response.get("payoutStatus"),
            "trio_payoff_status": odds_api_response.get("payoutStatus"),
            "exacta_payoff_status": odds_api_response.get("payoutStatus"),
            "quinella_payoff_status": odds_api_response.get("payoutStatus"),
            "quinella_place_payoff_status": odds_api_response.get("payoutStatus"),
            "bracket_exacta_payoff_status": odds_api_response.get("payoutStatus"),
            "bracket_quinella_payoff_status": odds_api_response.get("payoutStatus"),
            "is_aggregated": 1 if odds_api_response.get("isAggregated") else 0,
            "updated_at": self._to_timestamp(odds_api_response.get("updatedAt")),
            "odds_delayed": 1 if odds_api_response.get("oddsDelayed") else 0,
            "final_odds": 1 if odds_api_response.get("finalOdds") else 0,
        }
        transformed_data["odds_status"] = odds_status

        return transformed_data

    def _to_timestamp(self, datetime_obj) -> Optional[int]:
        """
        日時オブジェクトをタイムスタンプに変換
        """
        if datetime_obj is None:
            return None
        try:
            if isinstance(datetime_obj, str):
                from datetime import datetime

                dt = datetime.fromisoformat(datetime_obj.replace("Z", "+00:00"))
                return int(dt.timestamp())
            elif isinstance(datetime_obj, (int, float)):
                return int(datetime_obj)
            else:
                return None
        except Exception as e:
            self.logger.warning(f"タイムスタンプ変換エラー: {e}")
            return None
