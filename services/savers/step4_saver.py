"""
ステップ4: オッズ情報のデータセーバー (MySQL対応)
"""

import logging
from datetime import datetime  # timezone を削除
from typing import Any, Dict, List, Optional, Union

# KeirinDataAccessorをインポート
from database.db_accessor import KeirinDataAccessor  # パスは環境に合わせてください


class Step4Saver:
    def __init__(self, accessor: KeirinDataAccessor, logger: logging.Logger = None):
        self.accessor = accessor
        self.logger = logger or logging.getLogger(__name__)

        # オッズステータステーブルの情報
        self.odds_status_table = "odds_statuses"
        self.odds_status_pk = "race_id"
        # APIのオッズ更新タイムスタンプキー (APIレスポンスのルートレベルにあると仮定)
        self.api_odds_updated_at_key = "updatedAt"

        # オッズテーブルの設定（Step4Updaterと同じ設定を使用）
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

    def get_race_statuses(self, race_ids: List[str]) -> Dict[str, str]:
        """
        指定されたレースIDリストに対応する races テーブルの status を取得する。
        Args:
            race_ids: ステータスを取得したいレースIDのリスト。
        Returns:
            race_id をキー、status を値とする辞書。
        """
        if self.accessor is None:
            self.logger.error(
                f"Step4Saver ({self.__class__.__name__}): accessor is None before executing get_race_statuses"
            )
            raise AttributeError("Accessor is None, cannot proceed.")
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step4Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)})"
            )
            raise TypeError(
                f"Accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)})."
            )

        if not race_ids:
            self.logger.info("get_race_statuses: 取得対象のレースIDがありません。")
            return {}

        query = f"""
        SELECT race_id, status
        FROM races
        WHERE race_id IN ({", ".join(["%s"] * len(race_ids))})
        """
        params = tuple(race_ids)

        race_statuses: Dict[str, str] = {}
        try:
            results = self.accessor.execute_query(query, params)
            if results:
                for row in results:
                    race_statuses[str(row["race_id"])] = str(row["status"])
                self.logger.debug(
                    f"get_race_statuses: {len(results)}件のレースステータスを取得しました。"
                )
            else:
                self.logger.info(
                    f"get_race_statuses: 指定されたレースID ({len(race_ids)}件) に該当するレースがracesテーブルに見つかりませんでした。"
                )
        except Exception as e:
            self.logger.error(
                f"get_race_statuses: レースステータス取得中にエラー (IDs: {race_ids}): {e}",
                exc_info=True,
            )
            # エラーが発生しても、部分的に取得できたものは返すか、空を返すか。ここでは空を返す。
            return {}  # もしくはエラーを再raiseする
        return race_statuses

    def check_odds_update_history(self, race_ids: List[str]) -> Dict[str, bool]:
        """
        指定されたレースIDリストに対して、過去にオッズ更新履歴があるかを確認する。
        odds_statusesテーブルの存在をチェックし、過去に更新された履歴があるかを判定する。
        Args:
            race_ids: 更新履歴を確認したいレースIDのリスト。
        Returns:
            race_id をキー、更新履歴があるかどうか(bool)を値とする辞書。
        """
        if self.accessor is None:
            self.logger.error(
                f"Step4Saver ({self.__class__.__name__}): accessor is None before executing check_odds_update_history"
            )
            raise AttributeError("Accessor is None, cannot proceed.")

        if not race_ids:
            self.logger.info(
                "check_odds_update_history: 確認対象のレースIDがありません。"
            )
            return {}

        query = f"""
        SELECT race_id
        FROM {self.odds_status_table}
        WHERE race_id IN ({", ".join(["%s"] * len(race_ids))})
        """
        params = tuple(race_ids)

        history_results: Dict[str, bool] = {}
        # 初期化：すべてのレースIDをFalseで初期化
        for race_id in race_ids:
            history_results[race_id] = False

        try:
            results = self.accessor.execute_query(query, params)
            if results:
                for row in results:
                    history_results[str(row["race_id"])] = True
                self.logger.debug(
                    f"check_odds_update_history: {len(results)}件のレースに更新履歴が見つかりました。"
                )
            else:
                self.logger.debug(
                    f"check_odds_update_history: 指定されたレースID ({len(race_ids)}件) には更新履歴がありません。"
                )
        except Exception as e:
            self.logger.error(
                f"check_odds_update_history: 更新履歴確認中にエラー (IDs: {race_ids}): {e}",
                exc_info=True,
            )
            # エラーが発生した場合は、すべてFalseで返す（安全側に倒す）
            return {race_id: False for race_id in race_ids}

        return history_results

    def _to_timestamp(
        self,
        datetime_obj: Optional[Union[str, int, float, datetime]],
        default_value: Optional[int] = None,
    ) -> Optional[int]:
        if datetime_obj is None:
            return default_value

        if isinstance(datetime_obj, (int, float)):
            return int(datetime_obj)

        if isinstance(datetime_obj, datetime):
            return int(datetime_obj.timestamp())

        datetime_str = str(datetime_obj)

        try:
            if "T" in datetime_str:
                if datetime_str.endswith("Z"):
                    dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
                    return int(dt.timestamp())
                else:
                    try:
                        dt = datetime.fromisoformat(datetime_str)
                        return int(dt.timestamp())
                    except ValueError:
                        self.logger.warning(
                            f"_to_timestamp: datetime.fromisoformatでパース失敗 (Tあり): '{datetime_str}'。他の形式を試みます。"
                        )
                        pass

            if " " in datetime_str and ":" in datetime_str:
                try:
                    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
                    return int(dt.timestamp())
                except ValueError:
                    try:
                        dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f")
                        return int(dt.timestamp())
                    except ValueError:
                        self.logger.warning(
                            f"_to_timestamp: 'YYYY-MM-DD HH:MM:SS[.fff]'形式でのパース失敗: '{datetime_str}'。他の形式を試みます。"
                        )
                        pass

            if datetime_str.isdigit():
                try:
                    ts = int(datetime_str)
                    if 946684800 < ts < 2524608000:
                        return ts
                    else:
                        self.logger.warning(
                            f"_to_timestamp: 数字列 '{datetime_str}' はタイムスタンプとして不適切な範囲です。"
                        )
                except ValueError:
                    pass

            self.logger.error(
                f"_to_timestamp: 未知の時刻フォーマットまたはパース不可能な値です: '{datetime_str}' (型: {type(datetime_obj)})。default_value ({default_value}) を返します。"
            )
            return default_value

        except Exception as e:
            self.logger.error(
                f"_to_timestamp: 予期せぬエラー (入力: '{datetime_obj}', 型: {type(datetime_obj)}): {e}",
                exc_info=True,
            )
            return default_value

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

            db_row = {"race_id": race_id}

            # 組み合わせキーの処理 (api_combination_key と db_combination_col を使用)
            if config.get("api_combination_key") and config.get("db_combination_col"):
                combination_data = item_api_data.get(config["api_combination_key"])

                # keyフィールドの処理: APIでは"key"という名前で配列が来る
                if (
                    config["api_combination_key"] == "numbers"
                    and "key" in item_api_data
                ):
                    combination_data = item_api_data.get("key")
                elif (
                    config["api_combination_key"] == "brackets"
                    and "key" in item_api_data
                ):
                    combination_data = item_api_data.get("key")

                # combination_dataが配列の場合は正常処理
                if isinstance(combination_data, list) and all(
                    isinstance(x, (str, int)) for x in combination_data
                ):
                    combination_list = combination_data
                # combination_dataが文字列の場合（既存のデータベースで"[1, 2]"のような形式）
                elif isinstance(combination_data, str):
                    try:
                        # "[1, 2]"形式の文字列をパース
                        if combination_data.startswith(
                            "["
                        ) and combination_data.endswith("]"):
                            # JSON形式の配列をパース
                            import json

                            combination_list = json.loads(combination_data)
                        else:
                            # "1-2"形式の文字列を分割
                            combination_list = combination_data.split("-")
                    except (json.JSONDecodeError, ValueError):
                        self.logger.warning(
                            f"Race {race_id}, Table {config['table_name']}: 組み合わせキー文字列のパースに失敗: {combination_data}"
                        )
                        continue
                else:
                    self.logger.warning(
                        f"Race {race_id}, Table {config['table_name']}: 組み合わせキー({config['api_combination_key']}) が不正: {combination_data}"
                    )
                    continue

                # 配列を"-"区切りの文字列に変換
                sep = "-"
                sort_target_tables = [
                    "odds_quinella",
                    "odds_trio",
                    "odds_quinella_place",
                ]
                if config["table_name"] in sort_target_tables:
                    try:
                        combination_key_str = sep.join(
                            sorted(map(str, combination_list), key=int)
                        )
                    except ValueError:
                        combination_key_str = sep.join(
                            sorted(map(str, combination_list))
                        )
                else:
                    combination_key_str = sep.join(map(str, combination_list))

                db_row[config["db_combination_col"]] = combination_key_str

            # 主要オッズの処理 (api_main_odds_key と db_main_odds_col を使用)
            if config.get("api_main_odds_key") and config.get("db_main_odds_col"):
                odds_val = item_api_data.get(config["api_main_odds_key"])
                db_row[config["db_main_odds_col"]] = (
                    float(odds_val) if odds_val is not None else None
                )

            # 最小オッズの処理 (api_min_odds_key と db_min_odds_col を使用)
            if config.get("api_min_odds_key") and config.get("db_min_odds_col"):
                min_odds_val = item_api_data.get(config["api_min_odds_key"])
                db_row[config["db_min_odds_col"]] = (
                    float(min_odds_val) if min_odds_val is not None else None
                )

            # 最大オッズの処理 (api_max_odds_key と db_max_odds_col を使用)
            if config.get("api_max_odds_key") and config.get("db_max_odds_col"):
                max_odds_val = item_api_data.get(config["api_max_odds_key"])
                db_row[config["db_max_odds_col"]] = (
                    float(max_odds_val) if max_odds_val is not None else None
                )

            # 追加カラムの処理 (additional_cols_mapping を使用)
            if "additional_cols_mapping" in config:
                for db_col, api_col in config["additional_cols_mapping"].items():
                    val = item_api_data.get(api_col)

                    # list型の値は文字列に変換（MySQLにlist型を直接保存できないため）
                    if isinstance(val, list):
                        # 配列の場合は適切に文字列に変換
                        if all(isinstance(x, (str, int)) for x in val):
                            val = "-".join(map(str, val))
                        else:
                            val = str(val)  # フォールバック：文字列化

                    # 型変換が必要な場合はここで行う (例: int, float, bool)
                    # 'absent' は bool の可能性がある
                    if db_col == "absent":
                        db_row[db_col] = bool(val) if val is not None else None
                    elif val is not None:  # 他のカラムは型に応じて変換
                        try:
                            if isinstance(val, (int, float)):  # 数値型はそのまま
                                db_row[db_col] = val
                            elif (
                                isinstance(val, str)
                                and any(c.isdigit() for c in val)
                                and "." in val
                            ):  # 小数点を含む数字文字列
                                db_row[db_col] = float(val)
                            elif isinstance(val, str) and val.isdigit():  # 整数文字列
                                db_row[db_col] = int(val)
                            else:  # それ以外は文字列として扱う (またはエラーログ)
                                db_row[db_col] = val
                        except ValueError:
                            self.logger.warning(
                                f"Race {race_id}, Table {config['table_name']}, Column {db_col}: 値 '{val}' の型変換に失敗しました。"
                            )
                            db_row[db_col] = (
                                val  # 変換失敗時は元の値をそのまま入れるか、Noneにするか
                            )
                    else:
                        db_row[db_col] = None

            to_save_list.append(db_row)
        return to_save_list

    def _save_single_odds_type_batch_with_cursor(
        self,
        race_id: str,
        odds_api_list: List[Dict[str, Any]],
        config_key: str,
        batch_size: int,
        cursor,
    ):
        """指定されたcursorを使用して単一種類のオッズデータをバッチ保存"""
        if not odds_api_list:
            self.logger.info(
                f"(Cursor) レースID {race_id}, オッズタイプ '{config_key}': 保存するデータがありません。"
            )
            return

        config = self.odds_table_configs.get(config_key)
        if not config:
            self.logger.error(
                f"(Cursor) レースID {race_id}: 無効なオッズタイプキー '{config_key}' です。設定が見つかりません。"
            )
            raise ValueError(f"Invalid odds type key: {config_key}")

        table_name = config["table_name"]
        prepared_data = self._prepare_odds_data_for_batch(
            race_id, odds_api_list, config
        )

        if not prepared_data:
            self.logger.info(
                f"(Cursor) レースID {race_id}, オッズタイプ '{config_key}': 整形後、保存対象データなし。"
            )
            return

        all_db_cols = ["race_id", config["db_combination_col"]]
        if "db_main_odds_col" in config:
            all_db_cols.append(config["db_main_odds_col"])
        if "db_min_odds_col" in config:
            all_db_cols.append(config["db_min_odds_col"])
        if "db_max_odds_col" in config:
            all_db_cols.append(config["db_max_odds_col"])

        additional_db_cols = list(config.get("additional_cols_mapping", {}).values())
        all_db_cols.extend(additional_db_cols)
        all_db_cols = sorted(list(set(all_db_cols)))

        cols_sql = ", ".join([f"`{col}`" for col in all_db_cols])
        values_sql = ", ".join(["%s"] * len(all_db_cols))

        update_expressions = []
        for col in all_db_cols:
            is_pk = False
            if col == "race_id" or col == config["db_combination_col"]:
                is_pk = True

            if not is_pk:
                update_expressions.append(f"`{col}` = VALUES(`{col}`)")
        update_sql = ", ".join(update_expressions)

        query = f"""
        INSERT INTO {table_name} ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        params_list = []
        for item in prepared_data:
            params_list.append(tuple(item.get(db_col) for db_col in all_db_cols))

        try:
            for i in range(0, len(params_list), batch_size):
                chunk = params_list[i : i + batch_size]
                if chunk:
                    cursor.executemany(query, chunk)
            self.logger.info(
                f"(Cursor) レースID {race_id}, オッズタイプ '{config_key}': {len(params_list)}件を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) レースID {race_id}, オッズタイプ '{config_key}' 保存中にエラー: {e}",
                exc_info=True,
            )
            raise

    def _save_odds_statuses_batch_with_cursor(
        self, odds_statuses_data: List[Dict[str, Any]], batch_size: int, cursor
    ):
        """指定されたcursorを使用してオッズステータスデータをバッチ保存"""
        if not odds_statuses_data:
            self.logger.info("(Cursor) 保存するオッズステータスデータがありません。")
            return

        to_save = []
        for status_data_api in odds_statuses_data:
            race_id = str(status_data_api.get("race_id", ""))
            if not race_id:
                self.logger.warning(
                    f"(Cursor) オッズステータスデータに race_id がありません: {status_data_api}"
                )
                continue
            data = {
                "race_id": race_id,
                "trifecta_payoff_status": status_data_api.get("trifectaPayoffStatus"),
                "trio_payoff_status": status_data_api.get("trioPayoffStatus"),
                "exacta_payoff_status": status_data_api.get("exactaPayoffStatus"),
                "quinella_payoff_status": status_data_api.get("quinellaPayoffStatus"),
                "quinella_place_payoff_status": status_data_api.get(
                    "quinellaPlacePayoffStatus"
                ),
                "bracket_exacta_payoff_status": status_data_api.get(
                    "bracketExactaPayoffStatus"
                ),
                "bracket_quinella_payoff_status": status_data_api.get(
                    "bracketQuinellaPayoffStatus"
                ),
                "is_aggregated": 1 if status_data_api.get("isAggregated") else 0,
                "odds_updated_at_timestamp": self._to_timestamp(
                    status_data_api.get(self.api_odds_updated_at_key)
                ),
                "odds_delayed": 1 if status_data_api.get("oddsDelayed") else 0,
                "final_odds": 1 if status_data_api.get("finalOdds") else 0,
            }
            to_save.append(data)

        if not to_save:
            self.logger.info(
                "(Cursor) 整形後、保存対象のオッズステータスデータがありませんでした。"
            )
            return

        cols = [
            "race_id",
            "trifecta_payoff_status",
            "trio_payoff_status",
            "exacta_payoff_status",
            "quinella_payoff_status",
            "quinella_place_payoff_status",
            "bracket_exacta_payoff_status",
            "bracket_quinella_payoff_status",
            "is_aggregated",
            "odds_updated_at_timestamp",
            "odds_delayed",
            "final_odds",
        ]
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))
        update_sql_parts = [
            f"`{col}` = VALUES(`{col}`)" for col in cols if col != "race_id"
        ]
        update_sql = ", ".join(update_sql_parts)

        query = f"""
        INSERT INTO {self.odds_status_table} ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """
        params_list = [tuple(d.get(col) for col in cols) for d in to_save]

        try:
            for i in range(0, len(params_list), batch_size):
                chunk = params_list[i : i + batch_size]
                if chunk:
                    cursor.executemany(query, chunk)
            self.logger.info(
                f"(Cursor) {len(params_list)}件のオッズステータス情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) オッズステータス情報保存エラー: {e}", exc_info=True
            )
            raise

    def save_all_odds_for_race(
        self,
        race_id: str,
        formatted_odds_data: Dict[str, Any],
        batch_size: int,
    ) -> bool:
        """
        レースのオッズ情報を保存 (Updaterから整形済みデータを受け取る)
        Args:
            race_id: レースID
            formatted_odds_data: Updaterで整形済みのオッズデータ
            batch_size: バッチサイズ
        Returns:
            bool: 保存成功の場合はTrue
        """
        if self.accessor is None:
            self.logger.error(
                f"Step4Saver ({self.__class__.__name__}): accessor is None before executing save_all_odds_for_race for race_id {race_id}"
            )
            raise AttributeError("Accessor is None, cannot proceed.")
        if not isinstance(self.accessor, KeirinDataAccessor):
            self.logger.error(
                f"Step4Saver ({self.__class__.__name__}): accessor is not a KeirinDataAccessor instance (type: {type(self.accessor)}) before executing save_all_odds_for_race for race_id {race_id}"
            )
            raise TypeError("Accessor is not a KeirinDataAccessor instance.")

        def _save_in_transaction(conn, cursor):
            try:
                self.logger.info(
                    f"レースID {race_id}: オッズデータの保存を開始します（トランザクション内）。"
                )

                # 各オッズ種別のデータを保存
                odds_types = [
                    "exacta",
                    "quinella",
                    "quinellaPlace",
                    "trifecta",
                    "trio",
                    "bracketExacta",
                    "bracketQuinella",
                ]

                for odds_type in odds_types:
                    if (
                        odds_type in formatted_odds_data
                        and formatted_odds_data[odds_type]
                    ):
                        self._save_formatted_odds_batch_with_cursor(
                            race_id,
                            formatted_odds_data[odds_type],
                            odds_type,
                            batch_size,
                            cursor,
                        )

                # オッズステータスの保存
                if "odds_status" in formatted_odds_data:
                    self._save_odds_status_with_cursor(
                        formatted_odds_data["odds_status"], cursor
                    )

                self.logger.info(
                    f"レースID {race_id}: オッズデータの保存が完了しました（トランザクション内）。"
                )
                return True

            except Exception as e:
                self.logger.error(
                    f"レースID {race_id}: オッズデータ保存トランザクション内でエラー: {e}",
                    exc_info=True,
                )
                raise

        try:
            return self.accessor.execute_in_transaction(_save_in_transaction)
        except Exception as e:
            self.logger.error(
                f"レースID {race_id} のオッズデータトランザクション保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def update_race_step4_status_batch(self, race_ids: List[str], status: str) -> None:
        """
        指定されたレースIDリストの race_status.step4_status を更新する (MySQL用)
        更新前にFOR UPDATEでロックを取得する。
        """
        if not race_ids:
            self.logger.info("更新対象のレースIDがありません (Step4ステータス)。")
            return

        def _update_status_in_transaction(conn, cursor):
            updated_count = 0
            try:
                for race_id in race_ids:
                    lock_query = "SELECT race_id, step4_status FROM race_status WHERE race_id = %s FOR UPDATE"
                    locked_row = self.accessor.execute_query_for_update(
                        query=lock_query,
                        params=(race_id,),
                        fetch_one=True,
                        conn=conn,
                        cursor=cursor,
                    )

                    if locked_row:
                        self.logger.debug(
                            f"Race ID {race_id} をロックしました。現在のstep4_status: {locked_row.get('step4_status')}"
                        )
                        update_query = """
                        UPDATE race_status
                        SET step4_status = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE race_id = %s
                        """
                        valid_status = status[:10]
                        self.accessor.execute_many(
                            query=update_query,
                            params_list=[(valid_status, race_id)],
                            existing_conn=conn,
                            existing_cursor=cursor,
                        )
                        updated_count += 1
                        self.logger.info(
                            f"Race ID {race_id} のStep4ステータスを '{valid_status}' に更新準備完了。"
                        )
                    else:
                        self.logger.warning(
                            f"Race ID {race_id} はrace_statusテーブルに存在しないか、ロックできませんでした。"
                        )
                return updated_count
            finally:
                pass

        try:
            num_updated = self.accessor.execute_in_transaction(
                _update_status_in_transaction
            )
            self.logger.info(
                f"合計 {num_updated}/{len(race_ids)} 件のレースのStep4ステータス更新処理が完了しました。"
            )
        except Exception as e:
            self.logger.error(
                f"レースStep4ステータス更新トランザクション中にエラー (IDs: {race_ids}, Status: {status}): {e}",
                exc_info=True,
            )
            raise

    def _save_formatted_odds_batch_with_cursor(
        self,
        race_id: str,
        formatted_odds_list: List[Dict[str, Any]],
        odds_type: str,
        batch_size: int,
        cursor,
    ):
        """整形済みオッズデータをバッチ保存"""
        if not formatted_odds_list:
            self.logger.info(
                f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}': 保存するデータがありません。"
            )
            return

        # テーブル名のマッピング
        table_mapping = {
            "exacta": "odds_exacta",
            "quinella": "odds_quinella",
            "quinellaPlace": "odds_quinella_place",
            "trifecta": "odds_trifecta",
            "trio": "odds_trio",
            "bracketExacta": "odds_bracket_exacta",
            "bracketQuinella": "odds_bracket_quinella",
        }

        table_name = table_mapping.get(odds_type)
        if not table_name:
            self.logger.error(f"未知のオッズタイプ: {odds_type}")
            return

        # テーブル設定から許可されたカラムを取得
        config = self.odds_table_configs.get(odds_type)
        if not config:
            self.logger.error(f"未知のオッズタイプ: {odds_type}")
            return

        # 基本カラム（race_id, key, odds関連）
        allowed_cols = ["race_id", config["db_combination_col"]]
        if config.get("db_main_odds_col"):
            allowed_cols.append(config["db_main_odds_col"])
        if config.get("db_min_odds_col"):
            allowed_cols.append(config["db_min_odds_col"])
        if config.get("db_max_odds_col"):
            allowed_cols.append(config["db_max_odds_col"])

        # additional_cols_mappingから許可されたカラムを追加
        if "additional_cols_mapping" in config:
            for db_col in config["additional_cols_mapping"].keys():
                if db_col not in allowed_cols:
                    allowed_cols.append(db_col)

        # 最初のレコードから実際に存在するカラムのみをフィルタ
        if formatted_odds_list:
            existing_cols = list(formatted_odds_list[0].keys())
            # 許可されたカラムかつ実際に存在するカラムのみを使用
            all_cols = [col for col in allowed_cols if col in existing_cols]

            # デバッグ: race_idが含まれているかチェック
            if "race_id" not in existing_cols:
                self.logger.error(
                    f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}': formatted_odds_listにrace_idが含まれていません。existing_cols: {existing_cols}"
                )
                self.logger.error(
                    f"(Cursor) レースID {race_id}: 最初のレコード: {formatted_odds_list[0]}"
                )
                return

            if "race_id" not in all_cols:
                self.logger.error(
                    f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}': race_idがall_colsに含まれていません。allowed_cols: {allowed_cols}, existing_cols: {existing_cols}"
                )
                return

            cols_sql = ", ".join([f"`{col}`" for col in all_cols])
            values_sql = ", ".join(["%s"] * len(all_cols))

            # race_idとkeyを除いたカラムでUPDATE
            update_expressions = []
            for col in all_cols:
                if col not in ["race_id", "key"]:
                    update_expressions.append(f"`{col}` = VALUES(`{col}`)")
            update_sql = ", ".join(update_expressions)

            query = f"""
            INSERT INTO {table_name} ({cols_sql})
            VALUES ({values_sql})
            ON DUPLICATE KEY UPDATE {update_sql}
            """

            params_list = []
            for idx, item in enumerate(formatted_odds_list):
                # 各値についてlist型が含まれていないかチェック
                safe_values = []
                for col in all_cols:
                    val = item.get(col)

                    # NOT NULLフィールドの特別チェック
                    if col == "race_id" and (val is None or val == ""):
                        self.logger.error(
                            f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}', アイテム {idx}: race_idがNullまたは空です。アイテム: {item}"
                        )
                        return
                    if col == "type" and (val is None or val == ""):
                        self.logger.error(
                            f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}', アイテム {idx}: typeがNullまたは空です。アイテム: {item}"
                        )
                        return
                    if col == "key" and (val is None or val == ""):
                        self.logger.error(
                            f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}', アイテム {idx}: keyがNullまたは空です。アイテム: {item}"
                        )
                        return

                    # list型やdict型などMySQL非対応の型を文字列に変換
                    if isinstance(val, (list, dict)):
                        if isinstance(val, list) and all(
                            isinstance(x, (str, int)) for x in val
                        ):
                            val = "-".join(map(str, val))
                        else:
                            val = str(val)
                    safe_values.append(val)
                params_list.append(tuple(safe_values))

            try:
                for i in range(0, len(params_list), batch_size):
                    chunk = params_list[i : i + batch_size]
                    if chunk:
                        cursor.executemany(query, chunk)
                self.logger.info(
                    f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}': {len(params_list)}件を保存/更新しました。"
                )
            except Exception as e:
                self.logger.error(
                    f"(Cursor) レースID {race_id}, オッズタイプ '{odds_type}' 保存中にエラー: {e}",
                    exc_info=True,
                )
                raise

    def _save_odds_status_with_cursor(self, odds_status_data: Dict[str, Any], cursor):
        """オッズステータスデータを保存"""
        if not odds_status_data or not odds_status_data.get("race_id"):
            self.logger.info("(Cursor) 保存するオッズステータスデータがありません。")
            return

        cols = list(odds_status_data.keys())
        cols_sql = ", ".join([f"`{col}`" for col in cols])
        values_sql = ", ".join(["%s"] * len(cols))

        # race_idを除いたカラムでUPDATE
        update_expressions = []
        for col in cols:
            if col != "race_id":
                update_expressions.append(f"`{col}` = VALUES(`{col}`)")
        update_sql = ", ".join(update_expressions)

        query = f"""
        INSERT INTO {self.odds_status_table} ({cols_sql})
        VALUES ({values_sql})
        ON DUPLICATE KEY UPDATE {update_sql}
        """

        # 各値についてlist型が含まれていないかチェック
        safe_values = []
        for col in cols:
            val = odds_status_data.get(col)
            # list型やdict型などMySQL非対応の型を文字列に変換
            if isinstance(val, (list, dict)):
                if isinstance(val, list) and all(
                    isinstance(x, (str, int)) for x in val
                ):
                    val = "-".join(map(str, val))
                else:
                    val = str(val)
            safe_values.append(val)
        params = tuple(safe_values)

        try:
            cursor.execute(query, params)
            self.logger.info(
                f"(Cursor) レースID {odds_status_data['race_id']}: オッズステータス情報を保存/更新しました。"
            )
        except Exception as e:
            self.logger.error(
                f"(Cursor) レースID {odds_status_data['race_id']} のオッズステータス保存中にエラー: {e}",
                exc_info=True,
            )
            raise


# --- 以前の bulk_save_step4_data や save_race_odds, _parse_... メソッドは、
# 新しい save_all_odds_for_race と save_single_odds_type_batch に役割が統合・変更されたため削除。
