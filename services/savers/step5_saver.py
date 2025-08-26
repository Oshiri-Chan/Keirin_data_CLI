"""
ステップ5: HTMLパース結果 (レース結果、周回、コメント等) のデータセーバー (MySQL対応)
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from database.db_accessor import KeirinDataAccessor


class Step5Saver:
    def __init__(self, accessor: KeirinDataAccessor, logger: logging.Logger = None):
        self.accessor = accessor
        self.logger = logger or logging.getLogger(__name__)

    def _to_timestamp(self, datetime_str: Optional[str]) -> Optional[int]:
        if not datetime_str or datetime_str == "0000-00-00 00:00:00":
            return None
        try:
            if "T" in datetime_str:
                dt_obj = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            else:
                dt_obj = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            return int(dt_obj.timestamp())
        except ValueError:
            self.logger.warning(f"日時変換失敗: {datetime_str}")
            return None

    def _save_race_results_batch_with_cursor(
        self, race_id: str, race_results_data: List[Dict[str, Any]], cursor
    ):
        if not race_results_data:
            self.logger.info(f"(Cursor) レースID {race_id}: 保存するレース結果なし。")
            return

        to_save = []
        for res_data in race_results_data:
            # 必須フィールドのバリデーション
            bracket_number = res_data.get("bracket_number")
            if bracket_number is None:
                self.logger.warning(
                    f"(Cursor) レースID {race_id}: bracket_numberなし。スキップ: {res_data}"
                )
                continue

            # player_idの取得と検証（DBから取得したものを優先、なければスクレイプした値を使用）
            player_id = res_data.get("player_id") or res_data.get("player_id_scraped")
            if not player_id:
                self.logger.warning(
                    f"(Cursor) レースID {race_id}: player_idなし。スキップ: {res_data}"
                )
                continue

            # rankの処理（整数変換）
            rank_value = res_data.get("rank")
            rank_int = None
            if rank_value is not None:
                if isinstance(rank_value, int):
                    rank_int = rank_value
                elif isinstance(rank_value, str) and rank_value.isdigit():
                    rank_int = int(rank_value)

            # 実際のテーブルスキーマに合わせたデータ構造（Step5Updaterの出力と一致）
            data = {
                "race_id": race_id,
                "bracket_number": int(bracket_number),
                "rank": rank_int,
                "rank_text": res_data.get("rank_text", ""),
                "mark": res_data.get("mark", ""),
                "player_name": res_data.get("player_name", ""),
                "player_id": player_id,
                "age": res_data.get("age"),
                "prefecture": res_data.get("prefecture", ""),
                "period": res_data.get("period"),
                "class": res_data.get("class", ""),
                "diff": res_data.get("diff", ""),
                "time": res_data.get("time"),
                "last_lap_time": res_data.get("last_lap_time", ""),
                "winning_technique": res_data.get("winning_technique", ""),
                "symbols": res_data.get("symbols", ""),
                "win_factor": res_data.get("win_factor", ""),
                "personal_status": res_data.get("personal_status", ""),
            }
            to_save.append(data)

        if not to_save:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 有効なレース結果データなし。"
            )
            return

        # 実際のテーブルカラムに合わせたクエリ
        cols = [
            "race_id",
            "bracket_number",
            "rank",
            "rank_text",
            "mark",
            "player_name",
            "player_id",
            "age",
            "prefecture",
            "period",
            "class",
            "diff",
            "time",
            "last_lap_time",
            "winning_technique",
            "symbols",
            "win_factor",
            "personal_status",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)"
            for col in cols
            if col not in ["race_id", "bracket_number"]
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"INSERT INTO race_results ({cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"
        params_list = [tuple(d.get(col) for col in cols) for d in to_save]
        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Cursor) レースID {race_id}: {len(params_list)}件のレース結果を保存/更新。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) レースID {race_id} レース結果保存エラー: {e}", exc_info=True
            )
            raise

    def _save_lap_positions_batch_with_cursor(
        self, all_lap_records_for_saver: List[Dict[str, Any]], cursor
    ):
        if not all_lap_records_for_saver:
            self.logger.info("(Cursor) 保存する周回位置データなし。")
            return

        all_params_to_save = []
        processed_race_ids = set()

        for race_record in all_lap_records_for_saver:
            race_id = race_record.get("race_id")
            if not race_id:
                self.logger.warning(
                    f"(Cursor) race_idなしの周回レコードをスキップ: {race_record}"
                )
                continue

            # 複数のデータキーをチェック
            lap_positions_data = None

            # 1. lap_positionsキーをチェック
            if race_record.get("lap_positions"):
                lap_positions_data = race_record.get("lap_positions")
                self.logger.debug(
                    f"(Cursor) レースID {race_id}: lap_positionsキーからデータ取得"
                )

            # 2. dataキーをチェック（現在の構造に対応）
            elif race_record.get("data"):
                data_content = race_record.get("data")
                if isinstance(data_content, dict):
                    # step5_updater_old.pyで使用されていたlap_data_by_sectionがある場合
                    if data_content.get("lap_data_by_section"):
                        lap_data_by_section = data_content["lap_data_by_section"]
                        converted_lap_data = (
                            self._convert_lap_data_by_section_to_lap_positions(
                                race_id, lap_data_by_section
                            )
                        )
                        if converted_lap_data:
                            lap_positions_data = converted_lap_data
                            self.logger.debug(
                                f"(Cursor) レースID {race_id}: lap_data_by_sectionから変換したデータ取得"
                            )
                        else:
                            self.logger.warning(
                                f"(Cursor) レースID {race_id}: lap_data_by_sectionの変換に失敗"
                            )

                    # 新しい形式: dataキー内に直接セクション名（周回、赤板、等）がある場合
                    elif any(
                        key in data_content
                        for key in ["周回", "赤板", "打鐘", "HS", "BS"]
                    ):
                        # 直接セクション名がキーとして存在する場合
                        section_data = {
                            "lap_shuukai": data_content.get("周回"),
                            "lap_akaban": data_content.get("赤板"),
                            "lap_dasho": data_content.get("打鐘"),
                            "lap_hs": data_content.get("HS"),
                            "lap_bs": data_content.get("BS"),
                        }
                        # Noneを除去
                        section_data = {
                            k: v for k, v in section_data.items() if v is not None
                        }

                        converted_lap_data = (
                            self._convert_lap_data_by_section_to_lap_positions(
                                race_id, section_data
                            )
                        )
                        if converted_lap_data:
                            lap_positions_data = converted_lap_data
                            self.logger.debug(
                                f"(Cursor) レースID {race_id}: 直接セクションキーから変換したデータ取得"
                            )
                        else:
                            self.logger.warning(
                                f"(Cursor) レースID {race_id}: 直接セクションデータの変換に失敗"
                            )

                    # 通常のlap_positions構造
                    elif data_content.get("lap_positions"):
                        lap_positions_data = data_content.get("lap_positions")
                        self.logger.debug(
                            f"(Cursor) レースID {race_id}: data.lap_positionsキーからデータ取得"
                        )

                    else:
                        self.logger.warning(
                            f"(Cursor) レースID {race_id}: dataキー内に認識可能な周回データが見つかりません。利用可能なキー: {list(data_content.keys())}"
                        )
                        continue

                elif isinstance(data_content, list):
                    # データが直接リスト形式の場合
                    lap_positions_data = data_content
                    self.logger.debug(
                        f"(Cursor) レースID {race_id}: dataキーから直接リストデータ取得"
                    )
                else:
                    self.logger.warning(
                        f"(Cursor) レースID {race_id}: dataキーの内容が想定外の形式です。type: {type(data_content)}"
                    )
                    continue

            # 3. どちらのキーも見つからない場合
            else:
                self.logger.warning(
                    f"(Cursor) レースID {race_id}: 周回データなしでスキップ。利用可能なキー: {list(race_record.keys())}"
                )
                continue

            # lap_positions_dataが取得できた場合の処理
            if not lap_positions_data:
                self.logger.warning(f"(Cursor) レースID {race_id}: 周回データが空です")
                continue

            to_save_for_this_race = []
            for lap_data in lap_positions_data:
                lap_number = lap_data.get("lap_number")
                section_name = lap_data.get("section_name")
                player_order = lap_data.get("player_order_in_section")
                player_id = lap_data.get("player_id")

                if lap_number is None or section_name is None or player_order is None:
                    self.logger.warning(
                        f"(Cursor) レースID {race_id}: 周回位置PK情報不足: {lap_data}"
                    )
                    continue

                data = {
                    "race_id": race_id,
                    "lap_number": int(lap_number),
                    "section_name": str(section_name),
                    "player_order_in_section": int(player_order),
                    "player_id": str(player_id) if player_id else None,
                    "bracket_number_snapshot": (
                        int(
                            lap_data.get(
                                "bracket_number_snapshot",
                                lap_data.get(
                                    "bike_no", lap_data.get("bracket_number", 0)
                                ),
                            )
                        )
                        if lap_data.get(
                            "bracket_number_snapshot",
                            lap_data.get("bike_no", lap_data.get("bracket_number")),
                        )
                        is not None
                        else None
                    ),
                    "player_name_snapshot": str(
                        lap_data.get(
                            "player_name_snapshot",
                            lap_data.get("racer_name", lap_data.get("player_name", "")),
                        )
                    ),
                    "x_coord": (
                        int(lap_data.get("x_coord", lap_data.get("x_position", 0)))
                        if lap_data.get("x_coord", lap_data.get("x_position"))
                        is not None
                        else None
                    ),
                    "y_coord": (
                        int(lap_data.get("y_coord", lap_data.get("y_position", 0)))
                        if lap_data.get("y_coord", lap_data.get("y_position"))
                        is not None
                        else None
                    ),
                    "indicator_type": str(
                        lap_data.get(
                            "indicator_type",
                            lap_data.get("arrow", lap_data.get("has_arrow", "")),
                        )
                    ),
                }
                to_save_for_this_race.append(data)

            if to_save_for_this_race:
                all_params_to_save.extend(to_save_for_this_race)
                processed_race_ids.add(race_id)

        if not all_params_to_save:
            self.logger.info(
                "(Cursor) 全レース通じ、整形後保存対象の周回位置データなし。"
            )
            return

        # lap_positionsテーブルは既存スキーマに合わせてセクション別カラム構造を使用
        # データをrace_id単位でグループ化してセクション別JSONとして保存
        race_grouped_data = {}
        for data_item in all_params_to_save:
            race_id = data_item.get("race_id")
            if race_id not in race_grouped_data:
                race_grouped_data[race_id] = {
                    "lap_shuukai": [],
                    "lap_akaban": [],
                    "lap_dasho": [],
                    "lap_hs": [],
                    "lap_bs": [],
                }

            section_name = data_item.get("section_name", "")
            section_mapping = {
                "周回": "lap_shuukai",
                "赤板": "lap_akaban",
                "打鐘": "lap_dasho",
                "HS": "lap_hs",
                "BS": "lap_bs",
            }
            section_key = section_mapping.get(section_name)
            if section_key:
                race_grouped_data[race_id][section_key].append(data_item)

        # 各レースのデータを保存
        import json

        for race_id, sections in race_grouped_data.items():
            lap_data = {}
            for section_key, section_data in sections.items():
                if section_data:
                    # JSONシリアライズ可能な形式に変換
                    json_data = []
                    for item in sorted(
                        section_data, key=lambda x: x.get("player_order_in_section", 0)
                    ):
                        json_data.append(
                            [
                                item.get("bracket_number_snapshot"),
                                item.get("player_name_snapshot", ""),
                                item.get("x_coord", 0),
                                item.get("y_coord", 0),
                                item.get("indicator_type") == "arrow",
                            ]
                        )
                    lap_data[section_key] = json.dumps(json_data, ensure_ascii=False)

            if lap_data:
                cols = [
                    "race_id",
                    "lap_shuukai",
                    "lap_akaban",
                    "lap_dasho",
                    "lap_hs",
                    "lap_bs",
                ]
                values = [
                    race_id,
                    lap_data.get("lap_shuukai"),
                    lap_data.get("lap_akaban"),
                    lap_data.get("lap_dasho"),
                    lap_data.get("lap_hs"),
                    lap_data.get("lap_bs"),
                ]

                cols_sql = ", ".join([f"`{col}`" for col in cols])
                values_sql = ", ".join(["%s"] * len(cols))
                update_sql_parts = [
                    f"`{col}` = VALUES(`{col}`)" for col in cols[1:]
                ]  # race_id以外
                update_sql = ", ".join(update_sql_parts)
                query = f"INSERT INTO lap_positions ({cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"

                try:
                    cursor.execute(query, values)
                    self.logger.info(
                        f"(Cursor) レースID {race_id}: 周回位置データを保存/更新"
                    )
                except Exception as e:
                    self.logger.error(
                        f"(Cursor) レースID {race_id} 周回位置データ保存エラー: {e}",
                        exc_info=True,
                    )
                    raise

        # lap_positions が保存されたレースの is_processed を 1 に更新
        try:
            for processed_race_id in processed_race_ids:
                self._save_lap_data_status_with_cursor(
                    processed_race_id,
                    {"is_processed": 1},
                    cursor,
                )
            self.logger.info(
                f"(Cursor) 周回データ保存済みの {len(processed_race_ids)} レースについて lap_data_status.is_processed=1 を設定"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) lap_data_status の is_processed 更新中にエラー: {e}",
                exc_info=True,
            )

        # 処理完了ログ
        total_processed = sum(
            1 for sections in race_grouped_data.values() if any(sections.values())
        )
        self.logger.info(
            f"(Cursor) {total_processed}件の周回位置データを保存/更新。対象レース数: {len(processed_race_ids)}"
        )

    def _convert_lap_data_by_section_to_lap_positions(
        self, race_id: str, lap_data_by_section: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        セクション別の周回データをlap_positions形式に変換

        Args:
            race_id: レースID
            lap_data_by_section: セクション別の周回データ（JSON文字列の辞書、または直接データの辞書）

        Returns:
            変換されたlap_positionsデータのリスト
        """
        import json

        converted_data = []

        # セクション名のマッピング（step5_updater_old.pyと同じ）
        section_lap_mapping = {
            "lap_shuukai": ("周回", 1),
            "lap_akaban": ("赤板", 2),
            "lap_dasho": ("打鐘", 3),
            "lap_hs": ("HS", 4),
            "lap_bs": ("BS", 5),
        }

        for section_key, section_value in lap_data_by_section.items():
            if section_key not in section_lap_mapping:
                self.logger.warning(
                    f"(Cursor) レースID {race_id}: 不明なセクションキー '{section_key}' をスキップ"
                )
                continue

            section_name, lap_number = section_lap_mapping[section_key]

            try:
                # データがJSON文字列の場合はパース、そうでなければ直接使用
                if isinstance(section_value, str):
                    # JSON文字列をパース（step5_updater_old.pyの形式）
                    section_data = json.loads(section_value)
                elif isinstance(section_value, (list, dict)):
                    # 既にパースされた形式
                    section_data = section_value
                else:
                    self.logger.warning(
                        f"(Cursor) レースID {race_id}: セクション '{section_key}' のデータ形式が不明です。type: {type(section_value)}"
                    )
                    continue

                if not isinstance(section_data, list):
                    self.logger.warning(
                        f"(Cursor) レースID {race_id}: セクション '{section_key}' のデータがリスト形式ではありません"
                    )
                    continue

                # 各選手データを変換
                for player_order, player_data in enumerate(section_data, 1):
                    # step5_updater_old.pyの形式: [bracket_number, racer_name, x_position, y_position, has_arrow]
                    if isinstance(player_data, list) and len(player_data) >= 4:
                        bracket_number = player_data[0]
                        racer_name = player_data[1] if len(player_data) > 1 else ""
                        x_position = player_data[2] if len(player_data) > 2 else 0
                        y_position = player_data[3] if len(player_data) > 3 else 0
                        has_arrow = player_data[4] if len(player_data) > 4 else False

                        converted_entry = {
                            "lap_number": lap_number,
                            "section_name": section_name,
                            "player_order_in_section": player_order,
                            "player_id": None,  # step5_updater_old.pyの形式にはplayer_idが含まれていない
                            "bracket_number_snapshot": bracket_number,
                            "player_name_snapshot": racer_name,
                            "x_coord": x_position,
                            "y_coord": y_position,
                            "indicator_type": "arrow" if has_arrow else "",
                        }
                        converted_data.append(converted_entry)

                    # 新しい形式: 辞書形式のデータ
                    elif isinstance(player_data, dict):
                        bracket_number = (
                            player_data.get("bracket_number")
                            or player_data.get("car_number")
                            or player_data.get("bike_no")
                        )
                        racer_name = player_data.get("player_name") or player_data.get(
                            "racer_name", ""
                        )
                        x_position = player_data.get("x_coord") or player_data.get(
                            "x_position", 0
                        )
                        y_position = player_data.get("y_coord") or player_data.get(
                            "y_position", 0
                        )
                        has_arrow = player_data.get("has_arrow") or player_data.get(
                            "arrow", False
                        )

                        converted_entry = {
                            "lap_number": lap_number,
                            "section_name": section_name,
                            "player_order_in_section": player_order,
                            "player_id": player_data.get("player_id"),
                            "bracket_number_snapshot": bracket_number,
                            "player_name_snapshot": racer_name,
                            "x_coord": x_position,
                            "y_coord": y_position,
                            "indicator_type": "arrow" if has_arrow else "",
                        }
                        converted_data.append(converted_entry)

                    else:
                        self.logger.warning(
                            f"(Cursor) レースID {race_id}: セクション '{section_key}' の選手データが不正です: {player_data}"
                        )
                        continue

            except json.JSONDecodeError as e:
                self.logger.error(
                    f"(Cursor) レースID {race_id}: セクション '{section_key}' のJSON解析エラー: {e}"
                )
                continue
            except Exception as e:
                self.logger.error(
                    f"(Cursor) レースID {race_id}: セクション '{section_key}' の変換エラー: {e}"
                )
                continue

        self.logger.debug(
            f"(Cursor) レースID {race_id}: {len(converted_data)}件の周回データを変換しました"
        )
        return converted_data

    def _save_race_comments_batch_with_cursor(
        self, race_id: str, race_comments_data: List[Dict[str, Any]], cursor
    ):
        if not race_comments_data:
            self.logger.info(f"(Cursor) レースID {race_id}: 保存するコメントなし。")
            return

        first_item = race_comments_data[0]
        comment_text_val = ""

        if isinstance(first_item, dict):
            comment_text_val = str(
                first_item.get("comment_text", first_item.get("text", ""))
            )
        elif isinstance(first_item, str):
            comment_text_val = first_item
        else:
            self.logger.warning(
                f"(Cursor) レースID {race_id}: 不明なコメントデータ形式。保存をスキップ: {first_item}"
            )
            return

        to_save = {
            "race_id": race_id,
            "comment": comment_text_val,  # race_commentsテーブルではcommentカラムを使用
        }

        if not to_save["comment"]:
            self.logger.info(
                f"(Cursor) レースID {race_id}: コメント本文が空のため保存せず。"
            )
            return

        cols = ["race_id", "comment"]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "race_id"
        ]
        update_sql = ", ".join(update_sql_parts)
        query = f"INSERT INTO race_comments ({cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"
        params_list = [tuple(to_save.get(col) for col in cols)]
        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Cursor) レースID {race_id}: レースコメントを保存/更新。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) レースID {race_id} コメント保存エラー: {e}", exc_info=True
            )
            raise

    def _save_inspection_reports_batch_with_cursor(
        self, race_id: str, inspection_reports_data: List[Dict[str, Any]], cursor
    ):
        if not inspection_reports_data:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 保存する検車場レポートなし。"
            )
            return
        to_save = []
        for idx, report_data in enumerate(inspection_reports_data):
            comment_val = str(
                report_data.get(
                    "comment",
                    report_data.get("report_text", report_data.get("content", "")),
                )
            )
            if not comment_val:
                self.logger.info(
                    f"(Cursor) レースID {race_id}, データインデックス {idx}: 検車場レポート本文(comment)空。保存せず。"
                )
                continue

            # player_name_reported または player_id から選手名を取得
            # 選手名の取得と設定（6文字制限対応）
            player_name = ""
            if report_data.get("player_name_reported"):
                # 順位情報 "(1着)" などを除去
                raw_name = str(report_data["player_name_reported"])
                clean_name = re.sub(r"\([^)]*\)", "", raw_name).strip()
                # 6文字制限に合わせてトリケート
                player_name = clean_name[:6] if clean_name else ""
            elif report_data.get("player_id"):
                # フォールバックとしてplayer_idを使用（6文字制限）
                player_name = str(report_data["player_id"])[:6]

            data = {
                "race_id": race_id,
                "player": player_name,  # 6文字制限内の選手名を設定
                "comment": comment_val,
            }
            to_save.append(data)
        if not to_save:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 整形後、保存対象の検車場レポートなし。"
            )
            return

        cols = ["race_id", "player", "comment"]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col not in ["race_id"]
        ]
        update_sql = ", ".join(update_sql_parts)

        if not update_sql_parts:
            query = f"INSERT IGNORE INTO inspection_reports ({cols_sql}) VALUES ({values_sql})"
        else:
            query = f"INSERT INTO inspection_reports ({cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"

        params_list = [tuple(d.get(col) for col in cols) for d in to_save]
        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Cursor) レースID {race_id}: {len(params_list)}件の検車場レポートを保存/更新試行。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) レースID {race_id} 検車場レポート保存エラー: {e}",
                exc_info=True,
            )
            raise

    def _save_lap_data_status_with_cursor(
        self, race_id: str, lap_data_status_entry: Dict[str, Any], cursor
    ):
        """
        特定のレースIDに対する周回データステータスを保存/更新する (トランザクション内でcursorを使用)
        lap_data_status テーブルの主キーは race_id
        """
        if not lap_data_status_entry:
            self.logger.info(
                f"(Cursor) レースID {race_id}: 保存する周回データステータス情報なし。"
            )
            return

        # 入力データの整形と検証
        is_processed_val = lap_data_status_entry.get("is_processed")
        if isinstance(is_processed_val, bool):
            is_processed_db_val = 1 if is_processed_val else 0
        elif is_processed_val in (0, 1):
            is_processed_db_val = is_processed_val
        else:
            is_processed_db_val = None
            self.logger.warning(
                f"(Cursor) レースID {race_id}: is_processed の値が無効 ({is_processed_val})。DBデフォルトに依存します。"
            )

        last_checked_at_val = lap_data_status_entry.get("last_checked_at")
        last_checked_at_db_val = None
        if last_checked_at_val:
            timestamp = self._to_timestamp(str(last_checked_at_val))
            if timestamp:
                last_checked_at_db_val = datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
            else:
                self.logger.warning(
                    f"(Cursor) レースID {race_id}: last_checked_at の日時変換に失敗 ({last_checked_at_val})。NULLとして扱います。"
                )

        api_race_id = str(lap_data_status_entry.get("race_id", race_id))
        if api_race_id != race_id:
            self.logger.warning(
                f"(Cursor) 周回データステータスのrace_idが予期せぬ値です。引数: {race_id}, データ内: {api_race_id}。引数のrace_idを使用します。"
            )

        to_save = {
            "race_id": race_id,
            "is_processed": is_processed_db_val,
            "last_checked_at": last_checked_at_db_val,
        }

        cols = ["race_id", "is_processed", "last_checked_at"]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))

        update_col_statements = []
        if to_save["is_processed"] is not None:
            update_col_statements.append("`is_processed` = VALUES(`is_processed`)")
        if to_save["last_checked_at"] is not None:
            update_col_statements.append(
                "`last_checked_at` = VALUES(`last_checked_at`)"
            )

        update_sql = ", ".join(update_col_statements)

        if update_sql:
            query = f"INSERT INTO lap_data_status ({cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"
        else:
            query = (
                f"INSERT IGNORE INTO lap_data_status ({cols_sql}) VALUES ({values_sql})"
            )

        params_list = [tuple(to_save.get(col) for col in cols)]

        try:
            cursor.executemany(query, params_list)
            self.logger.info(
                f"(Cursor) レースID {race_id}: 周回データステータス情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) レースID {race_id} の周回データステータス情報保存中にエラー: {e}",
                exc_info=True,
            )
            raise

    def save_parsed_html_data(
        self,
        race_id: str,
        parsed_data: Dict[str, Any],  # Updaterから渡されるパース結果の包括的な辞書
        # 例: {'race_results': [...], 'lap_positions': [...], ...}
    ) -> bool:
        """
        パースされたHTMLデータ (レース結果、周回情報、コメント、検査レポート) をアトミックに保存する。
        """
        self.logger.info(
            f"レースID {race_id}: パースHTMLデータのアトミック保存処理を開始。"
        )

        def _save_in_transaction(conn, cursor):
            try:
                # 受け取った cursor を使用し、parsed_data の実データだけを保存
                if parsed_data and parsed_data.get("race_results"):
                    self._save_race_results_batch_with_cursor(
                        race_id,
                        parsed_data["race_results"],
                        cursor,
                    )
                if parsed_data and parsed_data.get("inspection_reports"):
                    self._save_inspection_reports_batch_with_cursor(
                        race_id,
                        parsed_data["inspection_reports"],
                        cursor,
                    )
                if parsed_data and parsed_data.get("race_comments"):
                    self._save_race_comments_batch_with_cursor(
                        race_id,
                        parsed_data["race_comments"],
                        cursor,
                    )
                if parsed_data and parsed_data.get("lap_positions"):
                    self._save_lap_positions_batch_with_cursor(
                        [{"race_id": race_id, "data": parsed_data["lap_positions"]}],
                        cursor,
                    )
                return True
            except Exception:
                raise

        try:
            return self.accessor.execute_in_transaction(
                _save_in_transaction
            )  # cursor引数を渡さない
        except Exception as e:
            self.logger.error(
                f"レースID {race_id} のパースHTMLデータのアトミック保存中に予期せぬエラー: {e}",
                exc_info=True,
            )
            return False

    def update_race_step5_status_batch(self, race_ids: List[str], status: str) -> None:
        """
        指定されたレースIDのリストに対して、lap_data_status テーブルの最終確認日時を更新する。
        is_processed フラグはこのメソッドでは変更しない。
        is_processed は save_parsed_html_data 内で周回情報保存時に設定される。

        Args:
            race_ids: 更新対象のレースIDのリスト
            status: 現在の処理ステータスを示す文字列（ログ記録用、is_processedには影響しない）
        """
        if not race_ids:
            self.logger.info(
                "lap_data_status の最終確認日時を更新するレースIDがありません。"
            )
            return

        def _update_status_in_transaction(conn, cursor):
            try:
                if race_ids:
                    # lap_data_status に対して last_checked_at を更新（無ければ作成）
                    upsert_sql = (
                        "INSERT INTO lap_data_status (race_id, last_checked_at) "
                        "VALUES (%s, NOW()) "
                        "ON DUPLICATE KEY UPDATE last_checked_at = VALUES(last_checked_at)"
                    )
                    params_list = [(race_id,) for race_id in race_ids]
                    cursor.executemany(upsert_sql, params_list)
                    self.logger.info(
                        f"{len(params_list)}件のレースIDについて lap_data_status.last_checked_at を更新/作成しました。ログ用status={status}"
                    )
                return True
            except Exception:
                raise

        try:
            self.accessor.execute_in_transaction(_update_status_in_transaction)
        except Exception as e:
            self.logger.error(
                f"lap_data_status の last_checked_at バッチ更新トランザクション全体でエラー: {e}",
                exc_info=True,
            )

    def save_race_results_batch(
        self, race_id: str, race_results_data: List[Dict[str, Any]]
    ) -> bool:
        """
        指定されたレースIDのレース結果を一括で保存/更新する。
        """
        if not race_results_data:
            self.logger.info(
                f"レースID {race_id}: 保存するレース結果データがありません。"
            )
            return True  # データがない場合は成功扱い

        def _save_results_in_transaction(conn, cursor):
            self._save_race_results_batch_with_cursor(
                race_id, race_results_data, cursor
            )
            return True

        try:
            # KeirinDataAccessor の execute_in_transaction を使用
            self.accessor.execute_in_transaction(_save_results_in_transaction)
            self.logger.info(
                f"レースID {race_id}: レース結果の保存トランザクションが正常に完了しました。"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"レースID {race_id}: レース結果の保存トランザクションでエラー: {e}",
                exc_info=True,
            )
            return False

    def save_inspection_reports_batch(
        self, race_id: str, inspection_reports_data: List[Dict[str, Any]]
    ) -> bool:
        """
        指定されたレースIDの検車場レポートを一括で保存/更新する。
        """
        if not inspection_reports_data:
            self.logger.info(
                f"レースID {race_id}: 保存する検車場レポートデータがありません。"
            )
            return True

        def _save_reports_in_transaction(conn, cursor):
            self._save_inspection_reports_batch_with_cursor(
                race_id, inspection_reports_data, cursor
            )
            return True

        try:
            self.accessor.execute_in_transaction(_save_reports_in_transaction)
            self.logger.info(
                f"レースID {race_id}: 検車場レポートの保存トランザクションが正常に完了しました。"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"レースID {race_id}: 検車場レポートの保存トランザクションでエラー: {e}",
                exc_info=True,
            )
            return False

    def save_race_comments_batch(
        self, race_id: str, race_comments_data: List[Dict[str, Any]]
    ) -> bool:
        """
        指定されたレースIDのレースコメントを一括で保存/更新する。
        （Updaterから直接呼び出されることを想定したラッパーメソッド）
        """
        if not race_comments_data:
            self.logger.info(
                f"レースID {race_id}: 保存するレースコメントデータがありません。"
            )
            return True  # データがない場合は成功扱い

        def _save_comments_in_transaction(conn, cursor):
            self._save_race_comments_batch_with_cursor(
                race_id, race_comments_data, cursor
            )
            return True

        try:
            self.accessor.execute_in_transaction(_save_comments_in_transaction)
            self.logger.info(
                f"レースID {race_id}: レースコメントの保存トランザクションが正常に完了しました。"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"レースID {race_id}: レースコメントの保存トランザクションでエラー: {e}",
                exc_info=True,
            )
            return False

    def save_lap_positions_batch(
        self, all_lap_data_for_saver: List[Dict[str, Any]]
    ) -> bool:
        """
        複数のレースIDにまたがる周回位置データを一括で保存/更新する。
        all_lap_data_for_saver は {'race_id': str, 'lap_positions': List[Dict]} のリスト。
        """
        if not all_lap_data_for_saver:
            self.logger.info("保存する周回位置データがありません。")
            return True  # データがない場合は成功扱い

        def _save_laps_in_transaction(conn, cursor):
            # _save_lap_positions_batch_with_cursor はこの形式のデータを期待している
            self._save_lap_positions_batch_with_cursor(all_lap_data_for_saver, cursor)
            return True

        try:
            self.accessor.execute_in_transaction(_save_laps_in_transaction)
            processed_race_ids_count = len(all_lap_data_for_saver)
            self.logger.info(
                f"{processed_race_ids_count} レース分の周回位置データの保存トランザクションが正常に完了しました。"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"周回位置データの保存トランザクションでエラー: {e}",
                exc_info=True,
            )
            return False


# 既存の _save_race_comment_batch, _save_html_results_batch, _save_inspection_report_batch, _save_lap_positions_batch
# および save_race_result_details, update_race_final_status は上記の新メソッド群に役割を吸収・改名されるため削除。
