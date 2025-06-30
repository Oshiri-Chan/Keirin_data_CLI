"""
ステップ3: レース詳細情報の取得・更新クラス (MySQL対応)
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set, Tuple

# Step3Saver と APIクライアント(仮に BaseKeirinAPI) をインポート
from services.savers.step3_saver import Step3Saver  # パスは環境に合わせてください

# from datetime import datetime # Step3Saverに移動したか、不要になったか


# from services.clients.base_keirin_api import BaseKeirinApi # APIクライアントの具体的なクラス名に置き換えてください

# --- バッチサイズの定義 ---
RACE_BATCH_SIZE = 50  # 一度にAPIから取得・処理するレース数 (API負荷と効率のバランス)

# 終了済みとみなすレースステータス (races.status の値)。実際の値に合わせてください。
FINISHED_RACE_STATUSES: Set[str] = {"3"}  # レース終了のサインが "3" であることを反映


class Step3Updater:
    """
    ステップ3: レース詳細情報 (出走表、選手情報など) を取得・更新するクラス (MySQL対応)
    """

    # def __init__(self, winticket_api, db_instance, saver=None, logger=None, max_workers=3, rate_limit_wait=1.0):
    def __init__(
        self,
        api_client: Any,
        saver: Step3Saver,
        logger: logging.Logger = None,
        max_workers: int = 3,
        rate_limit_wait: float = 1.0,
    ):
        """
        初期化
        Args:
            api_client: APIクライアントインスタンス
            saver (Step3Saver): Step3Saver のインスタンス
            logger (logging.Logger, optional): ロガーオブジェクト。 Defaults to None.
            max_workers (int): 並列処理の最大ワーカー数
            rate_limit_wait (float): API呼び出し間の待機時間（秒）
        """
        self.api = api_client
        # self.db = db_instance # db_instance は不要なので削除
        self.saver = saver  # Step3Saver のインスタンスを期待
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self.rate_limit_wait = rate_limit_wait

    def _fetch_race_detail_worker(
        self, race_identifier: Dict[str, Any]
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        単一レースの詳細情報 (出走表含む) をAPIから取得するワーカー関数
        Args:
            race_identifier (Dict): Extractorから渡される辞書。
                                     {'race_id': str, 'cup_id': str, 'race_number': int, 'race_index': int (APIのindex仕様に依存)}
        Returns:
            Tuple[str, Optional[Dict]]: (race_id, APIレスポンスデータ or None)
        """
        race_id = race_identifier.get("race_id")
        cup_id = race_identifier.get("cup_id")
        # API の get_race_info がどのパラメータを必要とするか確認 (例: day_index, race_number)
        race_index = race_identifier.get("race_index")
        race_number = race_identifier.get("race_number") or race_identifier.get(
            "number"
        )
        thread_id = threading.current_thread().ident

        # デバッグ用: 取得したパラメータをログ出力
        self.logger.debug(
            f"スレッド {thread_id}: パラメータ取得結果 - race_id: {race_id}, cup_id: {cup_id}, race_index: {race_index}, race_number: {race_number}"
        )

        # 必須パラメータのチェック (API仕様に応じて調整)
        if not all(
            [race_id, cup_id, race_index is not None, race_number is not None]
        ):  # race_day_index を race_index に変更
            self.logger.error(
                f"スレッド {thread_id}: レース識別情報が不完全です: {race_identifier}"
            )
            self.logger.error(
                f"スレッド {thread_id}: 不足情報詳細 - race_id: {race_id}, cup_id: {cup_id}, race_index: {race_index}, race_number: {race_number}"
            )
            return race_id, None

        try:
            self.logger.debug(
                f"スレッド {thread_id}: レースID {race_id} (Cup: {cup_id}, Index: {race_index}, No: {race_number}) の詳細情報取得開始"
            )  # DayIndex を Index に変更
            # APIクライアントのメソッド呼び出し (get_race_info の引数はAPI仕様に合わせる)
            race_detail_data = self.api.get_race_info(
                cup_id=cup_id, index=race_index, race_number=race_number
            )  # index パラメータに race_index を使用

            if (
                not race_detail_data
            ):  # APIエラー or 必須キー欠損の場合 (APIクライアント側でチェックとログ出力を期待)
                return race_id, None

            # APIレスポンス辞書に直接 race_id を追加 (Saver側で利用するため)
            race_detail_data["race_id"] = race_id
            self.logger.debug(
                f"スレッド {thread_id}: レースID {race_id} の詳細情報を取得完了 (race_id を付与)"
            )
            return race_id, race_detail_data
        except Exception as e:
            self.logger.error(
                f"スレッド {thread_id}: レースID {race_id} の詳細情報取得中にエラー: {e}",
                exc_info=True,
            )
            return race_id, None

    def update_races_step3(
        self,
        races_to_process: List[Dict[str, Any]],
        batch_size: int,
        with_parallel: bool = True,
        force_update: bool = False,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        指定されたレースリストのStep3詳細情報をAPIから取得し、バッチ処理で保存し、ステータスを更新する。
        Args:
            races_to_process (List[Dict[str, Any]]): 処理対象のレース情報リスト。
            batch_size (int): Saverに渡すDB保存時のバッチサイズ。
            with_parallel (bool): API取得を並列処理で行うか。
            force_update (bool): 強制更新フラグ。Trueの場合、終了済みレースも処理対象とする。
        Returns:
            tuple: (全体の成功フラグ, 結果詳細辞書)
        """
        if not races_to_process:
            self.logger.warning("更新するレースが指定されていません")
            # 結果詳細辞書に skipped_finished を追加
            return False, {
                "message": "No races provided",
                "total_races_input": 0,
                "succeeded_api_fetch": 0,
                "failed_api_fetch": 0,
                "skipped_finished": 0,
                "succeeded_saves": 0,
                "failed_saves": 0,
                "processed_races": 0,
            }

        thread_id = threading.current_thread().ident
        total_races_input = len(races_to_process)
        self.logger.info(
            f"スレッド {thread_id}: Step3 Updater 起動 (入力レース数: {total_races_input}) - バッチサイズ: {RACE_BATCH_SIZE}"
        )

        # 結果集計用
        race_ids_processed_input: Set[str] = {
            r["race_id"] for r in races_to_process if "race_id" in r
        }
        race_ids_api_success: Set[str] = set()
        race_ids_api_failed: Set[str] = set()
        race_ids_save_success: Set[str] = set()
        race_ids_save_failed: Set[str] = set()
        race_ids_skipped_finished: Set[str] = set()

        total_saved_players_all_batches = 0
        total_saved_entries_all_batches = 0
        total_saved_player_results_all_batches = 0
        total_saved_race_lines_all_batches = 0

        # --- 処理対象レースのフィルタリング (終了済みレースのスキップ) ---
        active_races_to_fetch_api: List[Dict[str, Any]] = []
        if race_ids_processed_input:
            try:
                # Step3Saver に get_race_statuses が実装されている前提
                current_race_main_statuses = self.saver.get_race_statuses(
                    list(race_ids_processed_input)
                )
            except AttributeError:
                self.logger.error(
                    "[Step3 Updater] self.saver に get_race_statuses メソッドが存在しません。レースステータス確認をスキップします。"
                )
                current_race_main_statuses = {}
            except Exception as e:
                self.logger.error(
                    f"[Step3 Updater] レースの主ステータス取得中にエラー: {e}",
                    exc_info=True,
                )
                current_race_main_statuses = {}

            for race_info in races_to_process:
                race_id = race_info.get("race_id")
                if not race_id:  # race_id がないデータはスキップ
                    self.logger.warning(
                        f"[Step3 Updater] race_id がないレース情報が見つかりました: {race_info}"
                    )
                    continue

                # force_update が True の場合は、ステータスチェックをスキップして常に処理対象とする
                if not force_update:
                    main_status = current_race_main_statuses.get(race_id)
                    if main_status and main_status in FINISHED_RACE_STATUSES:
                        self.logger.info(
                            f"[Step3 Updater] レースID {race_id} はステータス '{main_status}' のため、Step3処理をスキップします (終了済み扱い)。"
                        )
                        race_ids_skipped_finished.add(race_id)
                        continue  # スキップするので次のレースへ
                    elif (
                        not main_status
                    ):  # main_status が取得できなかった場合は警告しつつ処理対象（force_update=False時のみ）
                        self.logger.warning(
                            f"[Step3 Updater] レースID {race_id} の主ステータスが取得できませんでした。処理対象とします。"
                        )

                active_races_to_fetch_api.append(race_info)
        # --- フィルタリングここまで ---

        total_races_to_fetch = len(active_races_to_fetch_api)
        self.logger.info(
            f"[Step3 Updater] API取得対象レース数: {total_races_to_fetch} (入力: {total_races_input}, 終了済みスキップ: {len(race_ids_skipped_finished)})"
        )

        for i in range(0, total_races_to_fetch, RACE_BATCH_SIZE):
            current_batch_race_identifiers = active_races_to_fetch_api[
                i : i + RACE_BATCH_SIZE
            ]
            current_batch_race_ids_to_try = {
                r["race_id"] for r in current_batch_race_identifiers if "race_id" in r
            }
            batch_num = i // RACE_BATCH_SIZE + 1
            total_batches = (
                (total_races_to_fetch + RACE_BATCH_SIZE - 1) // RACE_BATCH_SIZE
                if total_races_to_fetch > 0
                else 0
            )
            self.logger.info(
                f"--- バッチ {batch_num}/{total_batches} を処理開始 ({len(current_batch_race_identifiers)}レース) --- stimulating API and DB"
            )

            # Step3Saverにステータス更新を依頼 ('processing')
            if current_batch_race_ids_to_try:
                try:
                    self.saver.update_race_step3_status_batch(
                        list(current_batch_race_ids_to_try), "processing"
                    )
                    self.logger.info(
                        f"[Step3 Updater] バッチ {batch_num}: {len(current_batch_race_ids_to_try)}件を 'processing' に更新。"
                    )
                except Exception as e:
                    self.logger.error(
                        f"[Step3 Updater] バッチ {batch_num}: 'processing' へのステータス更新中にエラー: {e}",
                        exc_info=True,
                    )
                    # 更新失敗したIDはAPI失敗としてマーク (このバッチのAPI取得をスキップするため)
                    race_ids_api_failed.update(current_batch_race_ids_to_try)
                    continue  # このバッチのAPI取得はスキップ

            # --- 1. バッチ内のデータ一括取得フェーズ ---
            api_responses_batch: List[Dict[str, Any]] = []
            # succeeded_fetch_batch_race_ids: List[str] = [] # 不要
            # failed_fetch_batch_race_ids: List[str] = list(current_batch_race_ids) # 不要

            futures_map = {}
            if (
                with_parallel
                and len(current_batch_race_identifiers) > 1
                and self.max_workers > 0
            ):
                self.logger.info(
                    f"バッチ {batch_num}: レース詳細情報の一括取得を並列処理で開始"
                )
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    for race_info in current_batch_race_identifiers:
                        # 'processing' 更新失敗でスキップされたレースは除外
                        if race_info.get("race_id") not in race_ids_api_failed:
                            futures_map[
                                executor.submit(
                                    self._fetch_race_detail_worker, race_info
                                )
                            ] = race_info.get("race_id")
            else:
                self.logger.info(
                    f"バッチ {batch_num}: レース詳細情報の一括取得を順次処理で開始"
                )
                for race_info in current_batch_race_identifiers:
                    if race_info.get("race_id") not in race_ids_api_failed:
                        # 逐次実行用のダミーFutureオブジェクト
                        class ImmediateFuture:
                            def __init__(self, result_val, _race_id_val):
                                self._result = result_val

                            def result(self):
                                return self._result

                            def __hash__(self):
                                return id(self)

                            def __eq__(self, other):
                                return id(self) == id(other)

                        result_tuple = self._fetch_race_detail_worker(race_info)
                        future_obj = ImmediateFuture(
                            result_tuple, race_info.get("race_id")
                        )
                        futures_map[future_obj] = race_info.get("race_id")
                        if (
                            self.rate_limit_wait > 0
                            and len(current_batch_race_identifiers) > 1
                        ):
                            time.sleep(self.rate_limit_wait)

            # 結果集計
            for future_obj, race_id_res_from_map in futures_map.items():
                if (
                    race_id_res_from_map in race_ids_api_failed
                ):  # 'processing' 更新失敗分はスキップ
                    continue
                try:
                    race_id_res, data = future_obj.result()
                    if data:
                        api_responses_batch.append(data)
                        race_ids_api_success.add(race_id_res)
                    else:  # if data: に対応する else
                        self.logger.warning(
                            f"[Step3 Updater] バッチ {batch_num}: Race ID {race_id_res} のAPI取得失敗またはデータ空。"
                        )
                        race_ids_api_failed.add(race_id_res)
                except Exception as e:  # try: に対応する except
                    self.logger.error(
                        f"[Step3 Updater] バッチ {batch_num}: Race ID {race_id_res_from_map} のFuture結果取得中エラー: {e}",
                        exc_info=True,
                    )
                    race_ids_api_failed.add(race_id_res_from_map)

            self.logger.info(
                f"バッチ {batch_num}: API取得結果 - 成功(データ有): {len(api_responses_batch)}件, API失敗/データ無: {len(current_batch_race_ids_to_try) - len(api_responses_batch)}件"
            )

            if not api_responses_batch:
                self.logger.warning(
                    f"バッチ {batch_num}: 有効なAPIレスポンスが0件。このバッチの保存処理をスキップします。"
                )
                # API取得失敗したレースは'failed'ステータスに (既に race_ids_api_failed に追加済み)
                # overall_batch_processing_success = False if race_ids_api_failed.intersection(current_batch_race_ids_to_try) else overall_batch_processing_success
                continue  # 次のバッチへ

            # --- 2. バッチ内のデータ抽出・整形フェーズ ---
            players_for_save_batch: List[Dict[str, Any]] = []
            entries_for_save_batch: List[Dict[str, Any]] = []
            player_records_for_save_batch: List[Dict[str, Any]] = []
            # race_lines_for_save_batch: List[Dict[str, Any]] = []  # 直接APIデータから処理するため削除

            for race_detail_from_api in api_responses_batch:
                current_race_id = race_detail_from_api.get("race_id")
                if not current_race_id:
                    self.logger.warning(
                        f"バッチ {batch_num}: race_id 不明のため保存スキップ: {race_detail_from_api.keys()}"
                    )
                    continue

                # APIレスポンスの構造に応じてキーを調整 (e.g., 'entries' or 'raceEntries')
                # players, entries, player_results, race_lines のデータ整形 (Step3Saverの期待する形式に)
                # 以下はStep3Saverのコメントや以前のUpdaterの整形ロジックを参考にした例

                # Players
                api_players = race_detail_from_api.get(
                    "players", []
                )  # APIの選手リストキー
                for p_api in api_players:
                    p_id = p_api.get("id")  # APIの選手IDキー
                    if p_id:
                        players_for_save_batch.append(
                            {
                                "race_id": current_race_id,  # ★ race_id を追加
                                "player_id": p_id,
                                "name": p_api.get("name"),
                                "class": p_api.get("class"),
                                "player_group": p_api.get("group"),
                                "prefecture": p_api.get("prefecture"),
                                "term": p_api.get("term"),
                                "region_id": p_api.get("regionId"),
                                "yomi": p_api.get("yomi"),
                                "birthday": p_api.get("birthday"),
                                "age": p_api.get("age"),
                                "gender": p_api.get("gender"),
                                # 'updated_at' はDB自動更新
                            }
                        )

                # Entries (出走表)
                api_entries = race_detail_from_api.get("entries", [])
                for e_api in api_entries:
                    entry_number = e_api.get("number")  # Saver の期待するキー 'number'
                    player_id_entry = e_api.get(
                        "playerId"
                    )  # Saver の期待するキー 'playerId'

                    if (
                        entry_number is not None
                    ):  # player_id は欠車の場合など None でもOK
                        entries_for_save_batch.append(
                            {
                                "number": self._safe_int_convert(entry_number),
                                "race_id": e_api.get(
                                    "raceId"
                                ),  # Saver側で使わないが、関連付けのため保持してもよい
                                "absent": (
                                    1 if e_api.get("absent") else 0
                                ),  # Saver の期待するキー 'absent'
                                "player_id": (
                                    str(player_id_entry) if player_id_entry else None
                                ),
                                "bracket_number": self._safe_int_convert(
                                    e_api.get("bracketNumber")
                                ),  # Saver の期待するキー 'bracketNumber'
                                "player_current_term_class": self._safe_int_convert(
                                    e_api.get("playerCurrentTermClass")
                                ),  # Saver の期待するキー
                                "player_current_term_group": self._safe_int_convert(
                                    e_api.get("playerCurrentTermGroup")
                                ),  # Saver の期待するキー
                                "player_previous_term_class": self._safe_int_convert(
                                    e_api.get("playerPreviousTermClass")
                                ),  # Saver の期待するキー
                                "player_previous_term_group": self._safe_int_convert(
                                    e_api.get("playerPreviousTermGroup")
                                ),  # Saver の期待するキー
                                "has_previous_class_group": (
                                    1 if e_api.get("hasPreviousClassGroup") else 0
                                ),  # Saver の期待するキー
                            }
                        )
                    else:
                        self.logger.warning(
                            f"レース {current_race_id}: 出走表データに車番(number)がありません。スキップ: {e_api}"
                        )

                # PlayerRecords (選手成績)
                api_player_records = race_detail_from_api.get("records", [])
                for pr_api in api_player_records:
                    player_id_record = pr_api.get("playerId")  # Saver の期待するキー
                    if player_id_record:
                        player_records_for_save_batch.append(
                            {
                                "race_id": current_race_id,  # APIからではなく、現在処理中のrace_idを使用
                                "player_id": str(player_id_record),
                                "gear_ratio": self._safe_float_convert(
                                    pr_api.get("gearRatio")
                                ),  # Saver の期待するキー
                                "style": str(
                                    pr_api.get("style", "")
                                ),  # Saver の期待するキー
                                "race_point": self._safe_float_convert(
                                    pr_api.get("racePoint")
                                ),  # Saver の期待するキー
                                "comment": str(
                                    pr_api.get("comment", "")
                                ),  # Saver の期待するキー
                                "prediction_mark": self._safe_int_convert(
                                    pr_api.get("predictionMark")
                                ),  # Saver の期待するキー
                                "first_rate": self._safe_float_convert(
                                    pr_api.get("firstRate")
                                ),  # Saver の期待するキー
                                "second_rate": self._safe_float_convert(
                                    pr_api.get("secondRate")
                                ),  # Saver の期待するキー
                                "third_rate": self._safe_float_convert(
                                    pr_api.get("thirdRate")
                                ),  # Saver の期待するキー
                                "has_modified_gear_ratio": (
                                    1 if pr_api.get("hasModifiedGearRatio") else 0
                                ),  # Saver の期待するキー
                                "modified_gear_ratio": self._safe_float_convert(
                                    pr_api.get("modifiedGearRatio")
                                ),  # Saver の期待するキー
                                "modified_gear_ratio_str": str(
                                    pr_api.get("modifiedGearRatioStr", "")
                                ),  # Saver の期待するキー
                                "gear_ratio_str": str(
                                    pr_api.get("gearRatioStr", "")
                                ),  # Saver の期待するキー
                                "race_point_str": str(
                                    pr_api.get("racePointStr", "")
                                ),  # Saver の期待するキー
                                "previous_cup_id": (
                                    str(pr_api.get("previousCupId", ""))
                                    if pr_api.get("previousCupId")
                                    else None
                                ),  # Saver の期待するキー
                            }
                        )
                    else:
                        self.logger.warning(
                            f"レース {current_race_id}: 選手成績データに player_id がありません。スキップ: {pr_api}"
                        )

                # RaceLines (ライン情報)
                # linePredictionは単一のオブジェクト（配列ではない）
                line_prediction_data = race_detail_from_api.get("linePrediction")
                if line_prediction_data and isinstance(line_prediction_data, dict):
                    lines_data = line_prediction_data.get("lines", [])

                    # linesをline_formation形式に変換
                    line_formation = self._parse_lines_to_formation(
                        lines_data, current_race_id
                    )

                    # ライン情報を元のAPIデータに追加（後でStep3Saverに直接渡すため）
                    line_prediction_data["lineFormation"] = line_formation

            self.logger.info(
                f"バッチ {batch_num}: データ整形完了 - Players: {len(players_for_save_batch)}, Entries: {len(entries_for_save_batch)}, PlayerRecords: {len(player_records_for_save_batch)}"
            )

            # --- 3. バッチ内のデータ一括保存フェーズ (レースごとに処理) ---
            # batch_save_success = True # バッチ全体の保存成否フラグ # 不要

            for race_detail in api_responses_batch:
                current_race_id = race_detail.get("race_id")
                if not current_race_id:  # 基本的には race_id はあるはず
                    self.logger.warning(
                        f"バッチ {batch_num}: race_id 不明のため保存スキップ: {race_detail.keys()}"
                    )
                    continue

                # このレースがAPI成功リストに含まれているか再確認 (念のため)
                if current_race_id not in race_ids_api_success:
                    self.logger.warning(
                        f"バッチ {batch_num}: Race ID {current_race_id} はAPI成功リストにないため保存をスキップします。"
                    )
                    race_ids_save_failed.add(
                        current_race_id
                    )  # API成功していないが、ここまで来た場合
                    continue

                # このレースに関連する players, entries, player_records を抽出
                # (players は API の player_id を使う必要あり)
                api_player_ids_in_race = {
                    str(p.get("id"))
                    for p in race_detail.get("players", [])
                    if p.get("id")
                }
                current_players = [
                    p
                    for p in players_for_save_batch
                    if str(p.get("player_id")) in api_player_ids_in_race
                    and p.get("race_id") == current_race_id
                ]

                # Entries と PlayerRecords は整形済みリストから race_id でフィルタリングできる
                # （ただし、整形時に race_id を含めておく必要がある -> 上記修正で対応済み）
                # current_entries = [e for e in entries_for_save_batch if e.get('race_id') == current_race_id]
                # player_id で関連付ける必要があるため、Saverに渡す直前でフィルタリングする
                api_entry_player_ids = {
                    str(e.get("playerId", e.get("player_id")))
                    for e in race_detail.get(
                        "entries", race_detail.get("raceEntries", [])
                    )
                    if e.get("playerId") or e.get("player_id")  # NULLでないもののみ
                }
                current_entries = [
                    e
                    for e in entries_for_save_batch
                    if (
                        e.get("player_id") is None
                        or str(e.get("player_id")) in api_entry_player_ids
                    )  # 欠車の場合はNULL可能
                    and e.get("race_id") == current_race_id
                ]

                # player_id で関連付ける
                api_record_player_ids = {
                    str(pr.get("playerId", pr.get("player_id")))
                    for pr in race_detail.get(
                        "records", race_detail.get("playerRaceResults", [])
                    )
                    if pr.get("playerId") or pr.get("player_id")  # NULLでないもののみ
                }
                current_player_records = [
                    pr
                    for pr in player_records_for_save_batch
                    if pr.get("player_id")
                    and str(pr.get("player_id")) in api_record_player_ids
                    and pr.get("race_id") == current_race_id
                ]

                # ログメッセージ修正 (Pylanceエラーの原因箇所を修正)
                # self.logger.debug(f"バッチ {batch_num}, レース {current_race_id}: 保存処理開始 (Players: {len(current_players)}, Entries: {len(current_entries)}, PResults: {len(current_player_results)}, Lines: {len(current_race_lines)}) ") # 古い行
                self.logger.debug(
                    f"バッチ {batch_num}, レース {current_race_id}: 保存処理開始 (Players: {len(current_players)}, Entries: {len(current_entries)}, PRecords: {len(current_player_records)}) "
                )  # 修正後の行

                try:
                    # Saverのメソッドをレースごとに呼び出す (race_id を渡す)
                    # saver.save_players_batch は race_id を引数に取るので、current_race_id を渡す
                    players_saved = True
                    entries_saved = True
                    player_records_saved = True

                    if current_players:
                        # ★ 修正: Saverのメソッド名と引数を合わせる
                        #   save_players_batch は リストと race_id を取る
                        #   save_entries_batch も同様に race_id を取る (Saverの実装確認推奨)
                        #   save_player_results_batch も同様
                        #   save_race_lines_batch も同様
                        #   戻り値 s_*, c_* は不要になる (例外で成否判断)
                        players_saved = self.saver.save_players_batch(
                            current_players, current_race_id, batch_size
                        )
                        if players_saved:
                            total_saved_players_all_batches += len(
                                current_players
                            )  # 成功と仮定
                    else:
                        self.logger.warning(
                            f"レース {current_race_id}: 保存する選手データがありません"
                        )

                    if current_entries:
                        # ★ 修正: 引数に current_race_id を追加 (Saver側の実装による)
                        # 仮に Step3Saver.save_entries_batch も race_id を要求すると想定
                        entries_saved = self.saver.save_entries_batch(
                            current_entries, current_race_id, batch_size
                        )
                        if entries_saved:
                            total_saved_entries_all_batches += len(current_entries)
                    else:
                        self.logger.warning(
                            f"レース {current_race_id}: 保存する出走データがありません"
                        )

                    if current_player_records:
                        # デバッグ: player_recordsの内容を確認
                        for i, pr in enumerate(
                            current_player_records[:3]
                        ):  # 最初の3件をチェック
                            self.logger.debug(
                                f"レース {current_race_id}: PlayerRecord[{i}] - race_id={pr.get('race_id')}, player_id={pr.get('player_id')}"
                            )

                        # ★ 修正: 引数に current_race_id を追加 (Saver側の実装による)
                        # 仮に Step3Saver.save_player_results_batch も race_id を要求すると想定
                        player_records_saved = self.saver.save_player_records_batch(
                            current_player_records, current_race_id, batch_size
                        )
                        if player_records_saved:
                            total_saved_player_results_all_batches += len(
                                current_player_records
                            )
                    else:
                        self.logger.warning(
                            f"レース {current_race_id}: 保存する選手成績データがありません"
                        )

                    # Line Predictions 保存 の直前に追加
                    self.logger.debug(
                        f"レース {current_race_id}: Checking for 'linePrediction'. Available keys: {list(race_detail.keys())}"
                    )
                    if "linePrediction" in race_detail:
                        self.logger.debug(
                            f"レース {current_race_id}: 'linePrediction' data raw: {race_detail['linePrediction']}"
                        )
                    else:
                        self.logger.debug(
                            f"レース {current_race_id}: 'linePrediction' key NOT FOUND in race_detail."
                        )

                    line_prediction_api_data = race_detail.get("linePrediction")
                    if line_prediction_api_data is not None:
                        # Updater側でAPIデータを整形してからSaverに渡す
                        line_type = str(line_prediction_api_data.get("lineType", ""))
                        line_formation = str(
                            line_prediction_api_data.get("lineFormation", "")
                        )

                        # lineFormationが空の場合、linesから生成
                        if not line_formation and "lines" in line_prediction_api_data:
                            lines_data = line_prediction_api_data.get("lines", [])
                            line_formation = self._parse_lines_to_formation(
                                lines_data, current_race_id
                            )

                        # Saver用の整形済みデータを作成
                        formatted_line_data = {
                            "lineType": line_type,
                            "lineFormation": line_formation,
                        }

                        # 整形済みデータをSaverに渡す
                        self.saver.save_line_predictions_batch(
                            formatted_line_data, current_race_id
                        )
                        total_saved_race_lines_all_batches += 1
                    else:
                        self.logger.debug(
                            f"レース {current_race_id}: linePrediction データがありません (None)。"
                        )

                    # 3つのテーブルすべてが成功した場合のみ、保存成功とみなす
                    all_tables_saved = (
                        players_saved and entries_saved and player_records_saved
                    )

                    if all_tables_saved:
                        self.logger.info(
                            f"バッチ {batch_num}, レース {current_race_id}: 全テーブル保存成功 (Players: {players_saved}, Entries: {entries_saved}, PlayerRecords: {player_records_saved})"
                        )
                        race_ids_save_success.add(current_race_id)
                    else:
                        self.logger.warning(
                            f"バッチ {batch_num}, レース {current_race_id}: 一部テーブル保存失敗 (Players: {players_saved}, Entries: {entries_saved}, PlayerRecords: {player_records_saved})"
                        )
                        race_ids_save_failed.add(current_race_id)

                except Exception as save_err_race:
                    self.logger.error(
                        f"バッチ {batch_num}, レース {current_race_id}: データ保存処理中にエラー: {save_err_race}",
                        exc_info=True,
                    )
                    # race_save_successful = False # 不要
                    # batch_save_success = False # 不要
                    # final_failed_race_ids.append(current_race_id) # race_ids_save_failed を使用
                    race_ids_save_failed.add(current_race_id)
                    # self.saver.update_race_step3_status_batch([current_race_id], 'failed') # ステータス更新は最後にまとめて

            self.logger.info(
                f"--- バッチ {batch_num}/{total_batches} 処理完了 --- stimulating API and DB"
            )
            time.sleep(0.1)  # バッチ間インターバル

        # --- 全バッチ処理完了後のステータス更新 ---
        self.logger.info("[Step3 Updater] 全バッチ処理完了。最終ステータス更新開始...")

        # 1. 保存成功したものを 'completed' に
        # (API成功し、かつ保存成功したもの)
        ids_to_mark_completed_from_save = list(race_ids_save_success)
        # 2. 終了済みでスキップしたものも 'completed' に
        ids_to_mark_completed_skipped = list(race_ids_skipped_finished)
        # 3. 最終的な 'completed' 対象 (重複排除)
        final_ids_to_mark_completed = list(
            set(ids_to_mark_completed_from_save + ids_to_mark_completed_skipped)
        )

        if final_ids_to_mark_completed:
            try:
                self.saver.update_race_step3_status_batch(
                    final_ids_to_mark_completed, "completed"
                )
                self.logger.info(
                    f"[Step3 Updater] {len(final_ids_to_mark_completed)}件のレースを 'completed' に更新しました。 (保存成功: {len(ids_to_mark_completed_from_save)}, 終了スキップ: {len(ids_to_mark_completed_skipped)})"
                )
            except Exception as e:
                self.logger.error(
                    f"[Step3 Updater] 'completed' ステータス更新中にエラー: {e}",
                    exc_info=True,
                )
                # 更新失敗したものは failed_overall に含める
                race_ids_save_failed.update(final_ids_to_mark_completed)

        # 4. API失敗、または保存失敗したものを 'failed' に
        # (ただし、既に completed になっていないもの)
        failed_overall = (race_ids_api_failed.union(race_ids_save_failed)) - set(
            final_ids_to_mark_completed
        )
        if failed_overall:
            try:
                self.saver.update_race_step3_status_batch(
                    list(failed_overall), "failed"
                )
                self.logger.info(
                    f"[Step3 Updater] {len(failed_overall)}件のレースを 'failed' に更新しました。"
                )
            except Exception as e:
                self.logger.error(
                    f"[Step3 Updater] 'failed' ステータス更新中にエラー: {e}",
                    exc_info=True,
                )

        self.logger.info("[Step3 Updater] 最終ステータス更新完了。")

        # 全体結果の集計とロギング
        succeeded_overall_count = len(final_ids_to_mark_completed)
        failed_overall_count = len(failed_overall)
        # overall_success は、入力されたもののうち、最終的に completed になったものの割合などで定義できるが、ここでは単純に失敗がなければ True とする
        final_overall_success = (
            (failed_overall_count == 0 and succeeded_overall_count > 0)
            if total_races_input > 0
            else True
        )

        result_message = "Step3 Update process completed."

        result_details = {
            "message": result_message,
            "total_races_input": total_races_input,
            "skipped_as_finished": len(race_ids_skipped_finished),
            "attempted_api_fetches": total_races_to_fetch,  # APIを試みた数
            "successful_api_fetches": len(race_ids_api_success),
            "failed_api_fetches": len(
                race_ids_api_failed.intersection(
                    race_ids_processed_input - race_ids_skipped_finished
                )
            ),  # API試行対象のうち失敗した数
            "successful_saves": len(race_ids_save_success),
            "failed_saves": len(
                race_ids_save_failed.intersection(race_ids_api_success)
            ),  # API成功したが保存失敗した数
            "final_completed_races": succeeded_overall_count,
            "final_failed_races": failed_overall_count,
            "processed_races": len(race_ids_save_success)
            + len(race_ids_skipped_finished),
            "saved_players_total": total_saved_players_all_batches,
            "saved_entries_total": total_saved_entries_all_batches,
            "saved_player_records_total": total_saved_player_results_all_batches,
            "saved_line_predictions_total": total_saved_race_lines_all_batches,
        }
        self.logger.info(
            f"{result_message} Summary: Input={total_races_input}, Skipped={len(race_ids_skipped_finished)}, AttemptedAPI={total_races_to_fetch}, FinalCompleted={succeeded_overall_count}, FinalFailed={failed_overall_count}"
        )
        self.logger.info(
            f"Saved counts - Players: {total_saved_players_all_batches}, Entries: {total_saved_entries_all_batches}, PlayerRecords: {total_saved_player_results_all_batches}, LinePredictions: {total_saved_race_lines_all_batches}"
        )

        return final_overall_success, result_details

    # 古いメソッド (update_race_detail, update_races_sequentially) は削除

    # --- ヘルパーメソッドを追加 ---
    def _safe_int_convert(
        self, value: Any, default: Optional[int] = None
    ) -> Optional[int]:
        if value is None or value == "":
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _safe_float_convert(
        self, value: Any, default: Optional[float] = None
    ) -> Optional[float]:
        if value is None or value == "":
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _parse_lines_to_formation(self, lines_data: list, race_id: str) -> str:
        """
        APIのlines形式からline_formation文字列に変換

        Args:
            lines_data: APIから取得したlinesデータ（選手番号の配列の配列）
            race_id: レースID（ログ用）

        Returns:
            str: line_formation文字列（例："1・2―[4・7]―6"）
        """
        if not lines_data:
            self.logger.debug(f"レース {race_id}: lines_dataが空です")
            return ""

        try:
            # デバッグ: 入力データの詳細を出力
            self.logger.debug(
                f"レース {race_id}: lines_data型={type(lines_data)}, 内容={lines_data}"
            )

            line_parts = []

            # lines_dataがdictの場合（数値キーのdict）
            if isinstance(lines_data, dict):
                # キーを数値として並び替え
                sorted_keys = sorted(
                    lines_data.keys(),
                    key=lambda x: int(x) if str(x).isdigit() else float("inf"),
                )
                self.logger.debug(
                    f"レース {race_id}: dict形式のlines、ソート済みキー={sorted_keys}"
                )

                for key in sorted_keys:
                    line_group = lines_data[key]
                    self.logger.debug(
                        f"レース {race_id}: キー '{key}' のライングループ={line_group}"
                    )
                    line_part = self._process_line_group(line_group, race_id)
                    if line_part:
                        line_parts.append(line_part)
                        self.logger.debug(
                            f"レース {race_id}: キー '{key}' から生成されたライン部分='{line_part}'"
                        )

            # lines_dataがlistの場合
            elif isinstance(lines_data, list):
                self.logger.debug(
                    f"レース {race_id}: list形式のlines、要素数={len(lines_data)}"
                )
                for i, line_group in enumerate(lines_data):
                    self.logger.debug(
                        f"レース {race_id}: インデックス {i} のライングループ={line_group}"
                    )
                    line_part = self._process_line_group(line_group, race_id)
                    if line_part:
                        line_parts.append(line_part)
                        self.logger.debug(
                            f"レース {race_id}: インデックス {i} から生成されたライン部分='{line_part}'"
                        )

            # 各ラインを"―"で結合
            line_formation = "―".join(line_parts) if line_parts else ""

            self.logger.info(
                f"レース {race_id}: lines {lines_data} から line_formation '{line_formation}' を生成しました"
            )
            return line_formation

        except Exception as e:
            self.logger.warning(
                f"レース {race_id}: lines {lines_data} から line_formation の生成に失敗: {e}"
            )
            return ""

    def _process_line_group(self, line_group: Any, race_id: str) -> str:
        """
        ライングループの処理

        Args:
            line_group: ラインデータ（dictまたはlist）
            race_id: レースID（ログ用）

        Returns:
            str: 処理されたライン部分
        """
        if not line_group:
            return ""

        try:
            self.logger.debug(
                f"レース {race_id}: _process_line_group 開始 line_group型={type(line_group)}, 内容={line_group}"
            )
            group_parts = []

            if isinstance(line_group, dict):
                self.logger.debug(f"レース {race_id}: dict形式のライングループを処理")
                # entriesがある場合
                if "entries" in line_group and isinstance(line_group["entries"], list):
                    self.logger.debug(
                        f"レース {race_id}: entriesキーを発見、要素数={len(line_group['entries'])}"
                    )
                    for entry_idx, entry in enumerate(line_group["entries"]):
                        if isinstance(entry, dict) and "numbers" in entry:
                            numbers = entry["numbers"]
                            self.logger.debug(
                                f"レース {race_id}: entry[{entry_idx}].numbers={numbers}"
                            )
                            if isinstance(numbers, list) and numbers:
                                if len(numbers) == 1:
                                    # 単一の数字の場合
                                    group_parts.append(str(numbers[0]))
                                    self.logger.debug(
                                        f"レース {race_id}: 単一数字 {numbers[0]} を追加"
                                    )
                                else:
                                    # 複数の数字の場合は[　]で括る
                                    numbers_str = "・".join(
                                        str(num) for num in sorted(numbers)
                                    )
                                    formatted_numbers = f"[{numbers_str}]"
                                    group_parts.append(formatted_numbers)
                                    self.logger.debug(
                                        f"レース {race_id}: 複数数字 {formatted_numbers} を追加"
                                    )

                # 直接numbersがある場合
                if "numbers" in line_group:
                    numbers = line_group["numbers"]
                    self.logger.debug(f"レース {race_id}: 直接numbers={numbers}")
                    if isinstance(numbers, list) and numbers:
                        if len(numbers) == 1:
                            # 単一の数字の場合
                            group_parts.append(str(numbers[0]))
                            self.logger.debug(
                                f"レース {race_id}: 直接単一数字 {numbers[0]} を追加"
                            )
                        else:
                            # 複数の数字の場合は[　]で括る
                            numbers_str = "・".join(str(num) for num in sorted(numbers))
                            formatted_numbers = f"[{numbers_str}]"
                            group_parts.append(formatted_numbers)
                            self.logger.debug(
                                f"レース {race_id}: 直接複数数字 {formatted_numbers} を追加"
                            )

            elif isinstance(line_group, list):
                self.logger.debug(
                    f"レース {race_id}: list形式のライングループを処理、要素数={len(line_group)}"
                )
                # 単純な配列の場合：[1, 2]
                if line_group:
                    if len(line_group) == 1:
                        group_parts.append(str(line_group[0]))
                        self.logger.debug(
                            f"レース {race_id}: list単一数字 {line_group[0]} を追加"
                        )
                    else:
                        numbers_str = "・".join(str(num) for num in sorted(line_group))
                        formatted_numbers = f"[{numbers_str}]"
                        group_parts.append(formatted_numbers)
                        self.logger.debug(
                            f"レース {race_id}: list複数数字 {formatted_numbers} を追加"
                        )

            # 各エントリーを"・"で結合
            result = "・".join(group_parts) if group_parts else ""
            self.logger.debug(
                f"レース {race_id}: _process_line_group 完了、result='{result}'"
            )
            return result

        except Exception as e:
            self.logger.warning(
                f"レース {race_id}: ライングループ {line_group} の処理に失敗: {e}"
            )
            return ""

    # --- 古いメソッドは削除またはコメントアウト ---
    # def update_race_info(...): pass
    # def update_races(...): pass
    # def update_races_from_db(...): pass
    # def update_cup_races(...): pass
