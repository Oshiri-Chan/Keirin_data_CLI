"""
ステップ5: 結果情報の取得・更新クラス
"""

import logging
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import threading

# import os # osモジュールは現在直接使用されていないため、一旦コメントアウト (必要なら復活)
# import concurrent.futures # concurrent.futures は ThreadPoolExecutor, as_completed で十分
from typing import Any, Dict, List, Optional, Set, Tuple

# from urllib.parse import urljoin # urljoin は _build_yenjoy_url で直接組み立てているため不要
from bs4 import BeautifulSoup

# from services.api.yenjoy_api_client import YenJoyApiClient # 古いインポートパス
from api.yenjoy_api import (  # YenJoyApiClient の正しいインポートパスとクラス名に修正
    YenjoyAPI,
)
from database.db_accessor import KeirinDataAccessor
from services.savers.step5_saver import Step5Saver

# import requests # requests は api_client が担当するので不要

# Step5DataExtractor は KeirinDataAccessor やパーサーに責務を移すため不要
# from database.extractors.extract_data_for_step5 import Step5DataExtractor


# --- 設定値 ---
YENJOY_BASE_URL = "https://www.yen-joy.net/"  # グローバル定数として定義

RACE_BATCH_SIZE_FOR_PROCESSING = 50
# SAVE_BATCH_SIZE = 200 # Saver側で管理するためUpdater側では不要


class Step5Updater:
    """
    ステップ5: 結果情報を取得・更新するクラス
    """

    def __init__(
        self,
        api_client: YenjoyAPI,
        step5_saver: Step5Saver,
        db_accessor: KeirinDataAccessor,
        logger: Optional[logging.Logger] = None,
        max_workers: int = 5,
        rate_limit_wait_html: float = 0.5,
    ):
        self.api_client = api_client
        self.saver = step5_saver
        self.db_accessor = db_accessor
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self.rate_limit_wait_html = rate_limit_wait_html
        # _processing_races_lock は update_results_bulk 内でローカルに使用するか、より粒度の細かいロックを検討
        # self._processing_races_lock = threading.RLock()

    def _build_yenjoy_url(
        self,
        cup_start_date_yyyymmdd: str,
        race_date_yyyymmdd: str,
        venue_code_str: str,
        race_number_str: str,
    ) -> Optional[str]:
        # 引数名をより具体的に変更
        if not all(
            [
                cup_start_date_yyyymmdd,
                race_date_yyyymmdd,
                venue_code_str,
                race_number_str,
            ]
        ):
            self.logger.error(
                f"URL構築失敗: 情報不備 cup_start_date={cup_start_date_yyyymmdd}, race_date={race_date_yyyymmdd}, venue_code={venue_code_str}, race_number={race_number_str}"
            )
            return None

        first_day_yyyymm = cup_start_date_yyyymmdd[:6]
        # venue_code_str, race_number_str は既に文字列かつ適切な桁数と仮定
        # (呼び出し元で zfill(2) されている venue_code_str はそのまま利用)

        return f"{YENJOY_BASE_URL}kaisai/race/result/detail/{first_day_yyyymm}/{venue_code_str}/{cup_start_date_yyyymmdd}/{race_date_yyyymmdd}/{race_number_str}"

    def _fetch_and_parse_race_html_worker(
        self, race_info: Dict[str, Any]
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        race_id = race_info.get("race_id")
        # race_date_db は YYYY-MM-DD 形式を想定 (DBから取得時)
        race_date_from_db = race_info.get(
            "race_date_db"
        )  # キー名を変更して明確化 (例: races.start_atから変換した日付)
        venue_code_from_db = race_info.get("venue_code")  # キー名を変更して明確化
        race_number_from_db = race_info.get("race_number")  # キー名を変更して明確化
        cup_start_date_from_db = race_info.get("cup_start_date_yyyymmdd")

        if not all(
            [
                race_id,
                race_date_from_db,
                venue_code_from_db,
                race_number_from_db,
                cup_start_date_from_db,
            ]
        ):
            self.logger.error(
                f"[Step5 Worker] Race ID {race_id}: レース情報が不完全 (cup_start_date含む): {race_info}"
            )
            return race_id, None

        try:
            # YYYY-MM-DD を YYYYMMDD に変換
            formatted_race_date_for_url = str(race_date_from_db).replace("-", "")
            formatted_cup_start_date_for_url = str(cup_start_date_from_db).replace(
                "-", ""
            )
            venue_code_for_url = str(venue_code_from_db).zfill(2)
            # レース番号の先頭のゼロを削除し、もし結果が空なら "0" にする
            race_number_str_temp = str(race_number_from_db).lstrip("0")
            race_number_for_url = race_number_str_temp if race_number_str_temp else "0"
        except Exception as e:
            self.logger.error(
                f"[Step5 Worker] Race ID {race_id}: URL用パラメータフォーマット失敗: {e}",
                exc_info=True,
            )
            return race_id, None

        yenjoy_url = self._build_yenjoy_url(
            formatted_cup_start_date_for_url,
            formatted_race_date_for_url,
            venue_code_for_url,
            race_number_for_url,
        )
        if not yenjoy_url:
            # _build_yenjoy_url内でエラーログが出るのでここでは不要
            return race_id, None

        html_content = None
        try:
            self.logger.debug(
                f"[Step5 Worker] Race ID {race_id}: HTML取得開始 URL: {yenjoy_url}"
            )
            # api_client のメソッド呼び出しはそのまま
            api_response = self.api_client.get_html_content(yenjoy_url)
            if (
                api_response
                and api_response.get("success")
                and api_response.get("content")
            ):
                html_content = api_response["content"]
            else:
                status_code = api_response.get("status_code") if api_response else "N/A"
                error_message = (
                    api_response.get("error") if api_response else "Unknown error"
                )
                self.logger.warning(
                    f"[Step5 Worker] Race ID {race_id}: HTML取得失敗. URL: {yenjoy_url}, Status: {status_code}, Error: {error_message}"
                )
                return race_id, None
        except Exception as e:
            self.logger.error(
                f"[Step5 Worker] Race ID {race_id}: HTML取得中エラー: {e}",
                exc_info=True,
            )
            return race_id, None

        if (
            not html_content
        ):  # このチェックは上記 api_response の条件分岐でカバーされるが一応残す
            self.logger.warning(
                f"[Step5 Worker] Race ID {race_id}: HTMLコンテンツが空です。 URL: {yenjoy_url}"
            )
            return race_id, None

        parsed_data_dict = None
        try:
            # race_info を渡すことで、パーサー内で選手名とIDの紐付けなどに利用可能にする
            parsed_data_dict = self._parse_yenjoy_result_html(
                html_content, race_id, race_info
            )

            if parsed_data_dict and parsed_data_dict.get("is_empty", False):
                self.logger.info(
                    f"[Step5 Worker] Race ID {race_id}: HTMLパース成功、ただし抽出データなし。URL: {yenjoy_url}"
                )
                # update_results_bulk 側でこのケースを処理するため、ここでは parsed_data_dict をそのまま返す
                return race_id, parsed_data_dict
            elif (
                not parsed_data_dict
            ):  # _parse_yenjoy_result_html が万が一 None を返した場合など
                self.logger.error(
                    f"[Step5 Worker] Race ID {race_id}: HTMLパース処理が予期せずNoneまたは不正な値を返しました。URL: {yenjoy_url}"
                )
                return race_id, None  # この場合は明確なエラーとして扱う

            # 有効なデータがある場合 (is_empty=False)
            return race_id, parsed_data_dict

        except (
            Exception
        ) as e:  # _parse_yenjoy_result_html 内でキャッチされなかった最上位の例外
            self.logger.error(
                f"[Step5 Worker] Race ID {race_id}: HTMLパース処理の呼び出し側で予期せぬエラー: {e}. URL: {yenjoy_url}",
                exc_info=True,
            )
            return race_id, None

    def _parse_yenjoy_result_html(
        self, html_content: str, race_id: str, race_info: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        self.logger.info(f"[Parser] Race ID {race_id}: HTML解析開始。")
        soup = BeautifulSoup(html_content, "html.parser")

        # 抽出データを格納する辞書 (Saverのメソッド名やDBカラム構成を意識)
        parsed_data = {
            "race_id": race_id,  # これは常に設定
            "race_results": [],
            "lap_positions": {},  # セクションごとの JSON データを格納 (lap_data_by_section から変更)
            "race_comments": [],
            "inspection_reports": [],
            "is_empty": False,  # 解析の結果、有効なデータがなかった場合に True
            "parse_error": False,  # 解析中に例外が発生した場合に True
            "problematic_rows": [],  # 解析に問題があった行のHTMLを記録（デバッグ用）
        }
        thread_id = threading.get_ident()

        # --- 0. (準備) 出走表情報をDBから取得し、車番と選手IDのマップを作成 ---
        player_id_map = self._get_player_id_map_from_db(race_id)

        self.logger.debug(f"[Thread-{thread_id}] Race {race_id}: HTML解析開始...")

        if not html_content:
            self.logger.warning(
                f"[Thread-{thread_id}] Race {race_id}: 解析する HTML コンテンツがありません。"
            )
            parsed_data["is_empty"] = True
            return parsed_data

        try:
            # --- 1. race_results テーブル用データの抽出 ---
            try:
                result_table = soup.find("table", class_="result-table-detail")
                if result_table:
                    tbody = result_table.find("tbody")
                    if tbody:
                        rows = tbody.find_all("tr")
                        if len(rows) > 2:  # ヘッダーが2行あることを期待
                            rows = rows[2:]  # ヘッダー行を2行除外
                            for row_index, row in enumerate(rows):
                                cells = row.find_all("td")
                                row_log_prefix = f"[Thread-{thread_id}] Race {race_id} Result Row {row_index+1}:"

                                if len(cells) >= 14:  # 勝敗因、個人状況まで期待
                                    rank_text = self._normalize_text(
                                        cells[0].get_text()
                                    )
                                    bracket_number_tag = cells[1].find(
                                        ["span", "div", "i"]
                                    )
                                    bracket_number_str = (
                                        self._normalize_text(
                                            bracket_number_tag.get_text()
                                        )
                                        if bracket_number_tag
                                        else None
                                    )
                                    bracket_number = self._safe_cast(
                                        bracket_number_str, int
                                    )
                                    if bracket_number is None:
                                        self.logger.error(
                                            f"{row_log_prefix} bracket_number が取得または変換できませんでした。元文字列: '{bracket_number_str}'。この行をスキップします。 Row HTML: {row}"
                                        )
                                        parsed_data["problematic_rows"].append(
                                            f"bracket_number is None: {row}"
                                        )
                                        continue

                                    mark = self._normalize_text(cells[2].get_text())
                                    player_name_tag = cells[3].find("a")
                                    player_name = (
                                        self._normalize_text(player_name_tag.get_text())
                                        if player_name_tag
                                        else self._normalize_text(cells[3].get_text())
                                    )
                                    player_link = (
                                        player_name_tag["href"]
                                        if player_name_tag
                                        and player_name_tag.has_attr("href")
                                        else None
                                    )
                                    player_id_match = (
                                        re.search(r"/(\d+)$", player_link)
                                        if player_link
                                        else None
                                    )
                                    player_id_from_link = (  # DBから引くまでの仮のID
                                        player_id_match.group(1)
                                        if player_id_match
                                        else None
                                    )

                                    age_str = self._normalize_text(cells[4].get_text())
                                    age = self._safe_cast(age_str, int)
                                    prefecture = self._normalize_text(
                                        cells[5].get_text()
                                    )
                                    period_str = self._normalize_text(
                                        cells[6].get_text()
                                    ).replace("期", "")
                                    period = self._safe_cast(period_str, int)
                                    player_class = self._normalize_text(
                                        cells[7].get_text()
                                    )
                                    diff = self._normalize_text(cells[8].get_text())
                                    # cells[9]は上がりタイム
                                    lap_time_str = self._normalize_text(
                                        cells[9].get_text()
                                    )

                                    winning_technique = self._normalize_text(
                                        cells[10].get_text()
                                    )
                                    symbols_jhb = self._normalize_text(
                                        cells[11].get_text()
                                    )
                                    win_factor = self._normalize_text(
                                        cells[12].get_text()
                                    )
                                    personal_status = self._normalize_text(
                                        cells[13].get_text()
                                    )

                                    rank = self._safe_cast(rank_text, int)

                                    # 上がりタイム情報の処理
                                    # timeには上がりタイムのdouble型を、last_lap_timeには同じ値のstr型を格納
                                    lap_time_double = self._safe_cast(
                                        lap_time_str, float
                                    )
                                    lap_time_text = (
                                        lap_time_str
                                        if lap_time_str and lap_time_str.strip()
                                        else ""
                                    )

                                    result_entry = {
                                        "race_id": race_id,
                                        "bracket_number": bracket_number,  # DBカラム名に合わせる
                                        "rank": rank,  # DBカラム名に合わせる
                                        "rank_text": rank_text,  # DBカラム名に合わせる
                                        "mark": mark,
                                        "player_name": player_name,  # DBカラム名に合わせる
                                        "player_id_scraped": player_id_from_link,  # スクレイプしたID
                                        "age": age,
                                        "prefecture": prefecture,
                                        "period": period,
                                        "class": player_class,  # DBカラム名に合わせる（classは予約語なのでバッククォート必要）
                                        "diff": diff,  # DBカラム名に合わせる
                                        "time": lap_time_double,  # 上がりタイムのdouble型
                                        "last_lap_time": lap_time_text,  # 上がりタイムのstr型
                                        "winning_technique": winning_technique,  # DBカラム名に合わせる
                                        "symbols": symbols_jhb,  # DBカラム名に合わせる
                                        "win_factor": win_factor,  # DBカラム名に合わせる
                                        "personal_status": personal_status,  # DBカラム名に合わせる
                                        # "player_id": player_id, # これは後でDBから引いたものをセット
                                    }

                                    # player_id をマップから取得して設定
                                    player_id_for_result = player_id_map.get(
                                        str(bracket_number)
                                    )
                                    if player_id_for_result:
                                        result_entry["player_id"] = player_id_for_result
                                    else:
                                        result_entry["player_id"] = (
                                            None  # マップにない場合は None
                                        )
                                        self.logger.warning(
                                            f"{row_log_prefix} 車番 {bracket_number} (スクレイプ名: {player_name}) に対応する player_id がDBマップから見つかりません。"
                                        )
                                    parsed_data["race_results"].append(result_entry)
                                else:
                                    self.logger.warning(
                                        f"{row_log_prefix} 結果テーブルの行のセル数が予期したものではありません ({len(cells)}件)。期待値: 14以上。 Row HTML: {row}"
                                    )
                                    parsed_data["problematic_rows"].append(
                                        f"Incorrect cell count ({len(cells)}): {row}"
                                    )
                        else:
                            self.logger.warning(
                                f"[Thread-{thread_id}] Race {race_id}: 結果テーブル <tbody> 内に行が不足しています (ヘッダー除き0行)。"
                            )
                    else:
                        self.logger.warning(
                            f"[Thread-{thread_id}] Race {race_id}: 結果テーブル内に <tbody> が見つかりませんでした。"
                        )
                else:
                    self.logger.warning(
                        f"[Thread-{thread_id}] Race {race_id}: 結果テーブル <table class='result-table-detail'> が見つかりませんでした。"
                    )
            except Exception as table_ex:
                self.logger.error(
                    f"[Thread-{thread_id}] Race {race_id}: 結果テーブル解析全体でエラー: {table_ex}",
                    exc_info=True,
                )
                parsed_data["parse_error"] = True

            # --- 2. レースコメント (race_comments) の抽出 ---
            # CSSセレクタ: table.result-pay tfoot td
            race_comment_text = None
            try:
                comment_element = soup.select_one(
                    "table.result-pay tfoot td"  # 変更後のセレクタ
                )
                if comment_element:
                    race_comment_text = self._normalize_text(
                        comment_element.get_text(strip=True)
                    )
                    if race_comment_text:
                        parsed_data["race_comments"] = (
                            {  # 以前はリストだったが、単一辞書に変更
                                "comment_text": race_comment_text,
                                "source": "yenjoy_html",
                            }
                        )
                        self.logger.info(
                            f"Race {race_id} ({thread_id}): レースコメントを抽出しました。"
                        )
                else:
                    self.logger.info(
                        f"Race {race_id} ({thread_id}): レースコメントの要素 (table.result-pay tfoot td) が見つかりません。"
                    )
            except Exception as e:
                self.logger.error(
                    f"Race {race_id} ({thread_id}): レースコメント抽出中にエラー: {e}",
                    exc_info=True,
                )

            # --- 3. 周回データ (lap_positions) の抽出 ---
            lap_data_main_wrapper = soup.find("div", class_="result-b-hyo-lap-wrapper")
            if not lap_data_main_wrapper:
                self.logger.info(
                    f"Race {race_id} ({thread_id}): 周回データのメインラッパー 'div.result-b-hyo-lap-wrapper' が見つかりません。"
                )
            else:
                # メインラッパーの中の各 'div.b-hyo' が周回ごとのブロック
                lap_blocks = lap_data_main_wrapper.find_all("div", class_="b-hyo")
                if not lap_blocks:
                    self.logger.info(
                        f"Race {race_id} ({thread_id}): メインラッパー内に周回ブロック 'div.b-hyo' が見つかりません。"
                    )
                else:
                    for block_index, lap_block_div in enumerate(lap_blocks):
                        # 各ブロック内で 'table.table.mawari' を探す
                        lap_detail_table = lap_block_div.find(
                            "table", class_="table mawari"
                        )  # クラス名を完全に指定
                        if not lap_detail_table:
                            self.logger.info(
                                f"Race {race_id} ({thread_id}) Block {block_index}: 周回詳細テーブル 'table.table.mawari' が 'div.b-hyo' 内に見つかりません。"
                            )
                            continue

                        th = lap_detail_table.find("th")
                        section_name_raw = (
                            f"UnknownSection_B{block_index}"  # デフォルト
                        )
                        if th:
                            # th の中のテキストとbrタグを処理してセクション名を取得
                            text_content = []
                            for content in th.contents:
                                if isinstance(content, str):
                                    text_content.append(content)
                                elif content.name == "br":
                                    text_content.append("\\n")  # 改行文字として扱う
                            full_text = "".join(text_content)
                            # 改行で分割し、各行を正規化して結合
                            section_parts = [
                                self._normalize_text(part.strip())
                                for part in full_text.split("\\n")
                                if part.strip()
                            ]
                            if section_parts:
                                section_name_raw = "".join(
                                    section_parts
                                )  # 例: "周回", "赤板" など
                            else:  # th があっても有効なテキストがなければデフォルト名
                                section_name_raw = (
                                    f"UnknownSection_ThExists_B{block_index}"
                                )

                        section_key = section_name_raw
                        if not section_key or section_key.startswith(
                            "UnknownSection"
                        ):  # UnknownSectionも実質無効とする
                            self.logger.warning(
                                f"Race {race_id} ({thread_id}) Block {block_index}: 不明または空の周回セクション名 ('{section_name_raw}')。スキップします。"
                            )
                            continue

                        section_player_list = []
                        # td.lap-wrapper 内の bike-icon-wrapper を探す
                        lap_wrapper_td = lap_detail_table.find(
                            "td", class_="lap-wrapper"
                        )
                        if not lap_wrapper_td:
                            self.logger.info(
                                f"Race {race_id} ({thread_id}) Block {block_index} Section '{section_key}': 'td.lap-wrapper' が見つかりません。"
                            )
                            continue

                        bike_icons = lap_wrapper_td.find_all(
                            "span", class_="bike-icon-wrapper"
                        )
                        for icon_wrapper in bike_icons:
                            classes = icon_wrapper.get("class", [])
                            bike_no_class = next(
                                (c for c in classes if c.startswith("bikeno-")),
                                None,
                            )
                            x_pos_class = next(
                                (c for c in classes if c.startswith("x-")), None
                            )
                            y_pos_class = next(
                                (c for c in classes if c.startswith("y-")), None
                            )

                            if bike_no_class and x_pos_class and y_pos_class:
                                try:
                                    bike_no_str = bike_no_class.split("-")[1]
                                    bracket_number = self._safe_cast(bike_no_str, int)
                                    x_position = self._safe_cast(
                                        x_pos_class.split("-")[1], int
                                    )
                                    y_position = self._safe_cast(
                                        y_pos_class.split("-")[1], int
                                    )

                                    racer_name_span = icon_wrapper.find(
                                        "span", class_="racer-nm"
                                    )
                                    racer_name = (
                                        self._normalize_text(racer_name_span.get_text())
                                        if racer_name_span
                                        else (
                                            "誘導員" if bike_no_str == "0" else "不明"
                                        )
                                    )

                                    # has_arrow の判定: bike-icon-wrapper の子要素に bike-icon クラスがあり、
                                    # さらにその bike-icon クラスに arrow クラスが含まれるか確認
                                    has_arrow = False
                                    bike_icon_element = icon_wrapper.find(
                                        "span", class_="bike-icon"
                                    )
                                    if (
                                        bike_icon_element
                                        and "arrow"
                                        in bike_icon_element.get("class", [])
                                    ):
                                        has_arrow = True

                                    if (
                                        bracket_number is not None
                                        and x_position is not None
                                        and y_position is not None
                                    ):
                                        section_player_list.append(
                                            [
                                                bracket_number,
                                                racer_name,
                                                x_position,
                                                y_position,
                                                has_arrow,
                                            ]
                                        )
                                    else:
                                        self.logger.warning(
                                            f"Race {race_id} ({thread_id}) Section {section_key}: 周回データの一部が欠損。Icon: {icon_wrapper}"
                                        )
                                except (IndexError, ValueError) as parse_err:
                                    self.logger.error(
                                        f"Race {race_id} ({thread_id}) Section {section_key}: 周回データ属性解析エラー: {parse_err}. Icon: {icon_wrapper}",
                                        exc_info=True,
                                    )
                        if section_player_list:
                            try:
                                json_string = json.dumps(
                                    section_player_list, ensure_ascii=False
                                )
                                parsed_data["lap_positions"][section_key] = json_string
                            except Exception as json_err:
                                self.logger.error(
                                    f"Race {race_id} ({thread_id}) Section {section_key}: JSON変換エラー: {json_err}",
                                    exc_info=True,
                                )
                        else:
                            self.logger.info(
                                f"Race {race_id} ({thread_id}) Section '{section_key}': bike_icons 内で有効な選手データが見つかりませんでした。"
                            )

            # --- 4. 検車場レポート (inspection_reports) の抽出 ---
            inspection_reports_list = []
            try:
                # 検車場レポートのテキストを取得
                report_paragraphs = soup.select("p.result-kensya-report-text")
                if report_paragraphs:
                    for p_tag in report_paragraphs:
                        report_text = self._normalize_text(p_tag.get_text(strip=True))
                        if report_text:
                            # 【選手名(着順)】「本文」形式で複数選手のレポートが含まれている場合の分割処理
                            individual_reports = self._parse_inspection_report_text(
                                report_text
                            )
                            if individual_reports:
                                inspection_reports_list.extend(individual_reports)
                                self.logger.debug(
                                    f"Race {race_id} ({thread_id}): {len(individual_reports)}件の個別レポートに分割しました。"
                                )
                            else:
                                # 分割できない場合は従来通り1件として処理
                                inspection_reports_list.append(
                                    {
                                        "player_id": None,
                                        "player_name_reported": None,
                                        "report_text": report_text,
                                    }
                                )
                                self.logger.debug(
                                    f"Race {race_id} ({thread_id}): 分割パターンに該当しないレポートを1件として追加。"
                                )

                    if inspection_reports_list:
                        parsed_data["inspection_reports"] = inspection_reports_list
                        self.logger.info(
                            f"Race {race_id} ({thread_id}): {len(inspection_reports_list)}件の検車場レポートを抽出しました。"
                        )
                else:
                    self.logger.info(
                        f"Race {race_id} ({thread_id}): 検車場レポートの要素 (p.result-kensya-report-text) が見つかりませんでした。"
                    )
            except Exception as e:
                self.logger.error(
                    f"Race {race_id} ({thread_id}): 検車場レポート抽出中にエラー: {e}",
                    exc_info=True,
                )

            # --- 5. inspection_reports の player_id 紐付け ---
            try:
                if (
                    parsed_data.get("inspection_reports")
                    and parsed_data.get("race_results")
                    and player_id_map
                ):  # player_id_map (車番->player_idのグローバルマップ) を利用

                    # race_results から {正規化選手名: 車番文字列} の中間マップを作成
                    player_name_to_car_number_map: Dict[str, str] = {}
                    for res_entry in parsed_data["race_results"]:
                        if (
                            res_entry.get("player_name_scraped")
                            and res_entry.get("car_number") is not None
                        ):
                            # 選手名を正規化 (スペース除去、NFKC正規化)
                            normalized_name_in_result = (
                                self._normalize_text(res_entry["player_name_scraped"])
                                .replace(" ", "")
                                .replace("　", "")
                            )
                            if (
                                normalized_name_in_result
                            ):  # 空文字でなければマップに追加
                                player_name_to_car_number_map[
                                    normalized_name_in_result
                                ] = str(
                                    res_entry["car_number"]
                                )  # 車番は文字列として扱う

                    if not player_name_to_car_number_map:
                        self.logger.warning(
                            f"[Thread-{thread_id}] Race {race_id}: player_name_to_car_number_map (検車場レポート紐付け用の中間マップ) が空か作成できませんでした。race_results の内容を確認してください。"
                        )
                    else:
                        map_keys_sample = list(player_name_to_car_number_map.keys())[:5]
                        self.logger.info(
                            f"[Thread-{thread_id}] Race {race_id}: player_name_to_car_number_map 作成完了。件数: {len(player_name_to_car_number_map)}, キーサンプル: {map_keys_sample}"
                        )

                    for report_idx, report_entry in enumerate(
                        parsed_data["inspection_reports"]
                    ):
                        if "player_id" not in report_entry:
                            report_entry["player_id"] = None  # 初期化

                        player_name_reported_raw = report_entry.get(
                            "player_name_reported", ""
                        )
                        if not player_name_reported_raw:
                            self.logger.debug(
                                f"[Thread-{thread_id}] Race {race_id} Report {report_idx}: player_name_reportedが空のためスキップ"
                            )
                            continue

                        # "選手名(着順)" から選手名部分のみを抽出し正規化
                        # 既にスペースは除去済みなので、(着順)部分のみ除去
                        match = re.match(
                            r"^([^(（]+)(?:[（(].*?[）)])?$", player_name_reported_raw
                        )
                        extracted_player_name_normalized = ""
                        if match:
                            player_name_part = match.group(1)
                            if player_name_part:
                                extracted_player_name_normalized = self._normalize_text(
                                    player_name_part.strip()
                                )
                        else:  # パターンに一致しない場合、元の報告された名前を正規化して使用
                            extracted_player_name_normalized = self._normalize_text(
                                player_name_reported_raw
                            )
                            self.logger.debug(
                                f"[Thread-{thread_id}] Race {race_id} Report {report_idx}: player_name_reported '{player_name_reported_raw}' は標準的な着順パターンにマッチしませんでした。正規化して使用: '{extracted_player_name_normalized}'"
                            )

                        if not extracted_player_name_normalized:
                            self.logger.warning(
                                f"[Thread-{thread_id}] Race {race_id} Report {report_idx}: 正規化後の検車場レポート選手名が空です。元: '{player_name_reported_raw}'"
                            )
                            continue

                        # 正規化された報告選手名を使って、まず「車番」を引く
                        car_number_for_report = player_name_to_car_number_map.get(
                            extracted_player_name_normalized
                        )

                        if car_number_for_report:
                            # 次に、その車番を使って player_id_map (車番->player_id) から player_id を引く
                            player_id_for_report = player_id_map.get(
                                car_number_for_report
                            )
                            if player_id_for_report:
                                report_entry["player_id"] = player_id_for_report
                                self.logger.debug(
                                    f"[Thread-{thread_id}] Race {race_id} Report {report_idx}: 検車場レポート紐付け成功 (選手名→車番→player_id): '{player_name_reported_raw}' (抽出名: '{extracted_player_name_normalized}') -> 車番 '{car_number_for_report}' -> player_id '{player_id_for_report}'"
                                )
                            else:
                                self.logger.warning(
                                    f"[Thread-{thread_id}] Race {race_id} Report {report_idx}: 検車場レポート紐付け失敗。選手名 '{extracted_player_name_normalized}' から引いた車番 '{car_number_for_report}' に対応する player_id が player_id_map (グローバル) に見つかりません。"
                                )
                        else:
                            self.logger.warning(
                                f"[Thread-{thread_id}] Race {race_id} Report {report_idx}: 検車場レポート紐付け失敗。正規化選手名 '{extracted_player_name_normalized}' (元: '{player_name_reported_raw}') が player_name_to_car_number_map に見つかりません。マップ候補(先頭5件): {list(player_name_to_car_number_map.keys())[:5]}"
                            )
                elif parsed_data.get(
                    "inspection_reports"
                ):  # レポートはあるが、race_results または player_id_map がない場合
                    status_msgs = []
                    if not parsed_data.get("race_results"):
                        status_msgs.append("race_resultsが空です")
                    if not player_id_map:
                        status_msgs.append(
                            "player_id_map (グローバル車番→player_idマップ) が空です"
                        )

                    self.logger.warning(
                        f"[Thread-{thread_id}] Race {race_id}: 検車場レポートは存在しますが、紐付けに必要な情報 ({', '.join(status_msgs)}) が不足しているため、player_id紐付けをスキップします。"
                    )
                    for report_entry in parsed_data["inspection_reports"]:
                        if "player_id" not in report_entry:
                            report_entry["player_id"] = None

            except Exception as report_linking_ex:
                self.logger.error(
                    f"[Thread-{thread_id}] Race {race_id}: 検車場レポートのplayer_id紐付け処理中に予期せぬエラー: {report_linking_ex}",
                    exc_info=True,
                )
                if parsed_data.get("inspection_reports"):
                    for report_entry in parsed_data["inspection_reports"]:
                        if "player_id" not in report_entry:
                            report_entry["player_id"] = None

            # データが実質的に空かどうかの判定
            is_effectively_empty = True
            if parsed_data.get("race_results"):
                is_effectively_empty = False
            if parsed_data.get("race_comments"):
                is_effectively_empty = False
            if parsed_data.get("inspection_reports"):
                is_effectively_empty = False
            if parsed_data.get("lap_positions"):
                is_effectively_empty = False

            parsed_data["is_empty"] = is_effectively_empty
            if (
                is_effectively_empty and not parsed_data["parse_error"]
            ):  # エラーがなく、かつ空の場合のみログ出力
                self.logger.info(
                    f"[Thread-{thread_id}] Race {race_id}: 解析の結果、有効なデータが見つかりませんでした。"
                )
            elif parsed_data["parse_error"]:
                self.logger.error(
                    f"[Thread-{thread_id}] Race {race_id}: 解析中にエラーが発生しました。詳細は先行するログを確認してください。"
                )

            return parsed_data

        except Exception as e:
            self.logger.error(
                f"[Thread-{thread_id}] Race {race_id}: HTML解析の最上位で予期せぬエラー: {e}",
                exc_info=True,
            )
            # 最上位エラー時は、is_empty=True, parse_error=True とし、他のデータは空で返す
            return {
                "race_id": race_id,
                "race_results": [],
                "race_comments": [],
                "inspection_reports": [],
                "lap_positions": {},
                "is_empty": True,
                "parse_error": True,
                "problematic_rows": [],
            }

    def _normalize_text(self, text: Optional[str]) -> str:
        """
        テキストを正規化する (NFKC正規化、strip、Noneチェック)
        """
        if text is None:
            return ""
        try:
            # NFKC正規化（全角英数記号を半角に、半角カナを全角カナに等）
            normalized = unicodedata.normalize("NFKC", str(text))
            # 連続する空白を1つにまとめ、前後の空白を削除
            return " ".join(normalized.split())
        except TypeError:  # str(text) でエラーになるケースはほぼないはずだが念のため
            return ""  # エラー時は空文字を返す

    def _safe_cast(
        self, value: Optional[str], cast_type: type, default: Any = None
    ) -> Optional[Any]:
        """
        文字列を指定された型に安全にキャストする。失敗時は default を返す。
        """
        if value is None:
            return default
        try:
            return cast_type(value)
        except (ValueError, TypeError):
            # self.logger.debug(f"Safe cast failed for value '{value}' to type {cast_type}. Returning default '{default}'.")
            return default

    def _parse_inspection_report_text(self, report_text: str) -> List[Dict[str, Any]]:
        """
        検車場レポートテキストを解析し、【選手名(着順)】「本文」形式から
        選手ごとの個別レポートに分割する。

        Args:
            report_text: 検車場レポートの全体テキスト

        Returns:
            個別レポートの辞書のリスト
        """
        if not report_text:
            return []

        individual_reports = []

        # 【選手名(着順)】「本文」のパターンで分割
        # より柔軟なパターン: 【】の後に「」があることを前提とし、次の【まで、または文末までを本文とする
        pattern = r"【([^】]+)】「([^」]*?)」?(?=【|$)"
        matches = re.findall(pattern, report_text, re.DOTALL)

        # 上記パターンでマッチしない場合は、「」なしのパターンも試行
        if not matches:
            # 【選手名(着順)】の後、次の【まで、または文末までを本文とする
            pattern_no_quotes = r"【([^】]+)】([^【]*?)(?=【|$)"
            matches = re.findall(pattern_no_quotes, report_text, re.DOTALL)

        if matches:
            for match in matches:
                player_name_with_rank = match[0].strip()  # 例: "西岡 拓朗(1着)"
                report_content = match[1].strip()  # 例: "昨日は風が強くて..."

                if player_name_with_rank and report_content:
                    # 選手名からスペースを削除し、(着順)部分も含めて格納
                    # player_name_with_rankから選手名のみを抽出する場合は、
                    # 別途 (着順) 部分を除去する処理を追加
                    player_name_clean = player_name_with_rank.replace(" ", "").replace(
                        "　", ""
                    )

                    individual_reports.append(
                        {
                            "player_id": None,  # この段階では不明
                            "player_name_reported": player_name_clean,  # 例: "西岡拓朗(1着)"
                            "report_text": report_content,
                        }
                    )
        else:
            # パターンにマッチしない場合は、別のパターンを試行
            # 【】が使われていない場合の処理（必要に応じて実装）
            self.logger.debug(
                f"検車場レポートが【】「」パターンにマッチしませんでした: {report_text[:100]}..."
            )

        return individual_reports

    def _get_player_id_map_from_db(self, race_id: str) -> Dict[str, str]:
        """
        指定されたレースIDの出走表情報をDBから取得し、
        {車番文字列: player_id文字列} のマップを返す。
        """
        player_id_map: Dict[str, str] = {}
        if not self.db_accessor:
            self.logger.error(
                f"Race ID {race_id}: db_accessorが初期化されていません。player_idマップを取得できません。"
            )
            return player_id_map
        try:
            # KeirinDataAccessorのget_entries_for_raceメソッドなど、
            # 適切なメソッドが存在すればそれを使用するべきだが、ここでは直接クエリを実行する例を示す。
            # 実際にはdb_accessorに専用メソッドを実装することが望ましい。
            query = "SELECT number, player_id FROM entries WHERE race_id = %s"
            # execute_query は params をタプルで受け取る想定
            entries_raw = self.db_accessor.execute_query(query, params=(race_id,))
            self.logger.info(
                f"Race ID {race_id}: execute_query returned for entries: {entries_raw}"
            )

            if entries_raw:
                for entry in entries_raw:
                    # `number` と `player_id` が存在し、`number` が None でないことを確認
                    if (
                        entry
                        and isinstance(entry, dict)
                        and "number" in entry
                        and entry["number"] is not None
                        and "player_id" in entry
                        and entry["player_id"] is not None
                    ):
                        player_id_map[str(entry["number"])] = str(entry["player_id"])
                        self.logger.info(
                            f"Race ID {race_id}: player_id_map に追加: {str(entry['number'])} -> {str(entry['player_id'])}"
                        )
                    else:
                        self.logger.warning(
                            f"Race ID {race_id}: 不完全なエントリー情報のためスキップ: {entry}"
                        )
                if (
                    not player_id_map
                ):  # entries_raw はあったが、有効なデータがなかった場合
                    self.logger.warning(
                        f"Race ID {race_id}: DBから出走表情報を取得しましたが、有効な車番-選手IDのペアが見つかりませんでした。"
                    )
            else:
                self.logger.warning(
                    f"Race ID {race_id}: DBから出走表情報が見つかりませんでした (entries_raw is None or empty)。player_id の紐付けができません。"
                )
        except Exception as e:
            self.logger.error(
                f"Race ID {race_id}: 出走表情報取得中にエラー: {e}", exc_info=True
            )
        if player_id_map:  # マップが空でなければデバッグログ
            self.logger.debug(
                f"Race ID {race_id}: 作成された player_id_map (最初の5件): {list(player_id_map.items())[:5]}"
            )
        return player_id_map

    def update_results_bulk(
        self,
        start_date_str: str,
        end_date_str: str,
        venue_codes: Optional[List[str]] = None,
        force_update_all: bool = False,
    ) -> Dict[str, Any]:
        self.logger.info(
            f"[Step5 Updater] バルク更新開始: {start_date_str} - {end_date_str}, 会場: {venue_codes or '全会場'}, 強制: {force_update_all}"
        )

        try:
            races_to_process_info = (
                self.db_accessor.get_yenjoy_races_to_update_for_step5(
                    start_date_str, end_date_str, venue_codes, force_update_all
                )
            )
        except Exception as e:
            self.logger.error(f"更新対象レース取得エラー: {e}", exc_info=True)
            return {
                "success": False,
                "message": "DB Error: Failed to get races for Step5.",
                "details": {},
            }

        if not races_to_process_info:
            self.logger.info("Step5 更新対象レースなし。")
            return {
                "success": True,
                "message": "No races to update for Step5.",
                "details": {},
            }
        self.logger.info(f"Step5 更新対象レース数: {len(races_to_process_info)}")

        all_parsed_data_from_html: List[Dict[str, Any]] = []
        successful_html_parse_ids: Set[str] = set()
        failed_html_parse_ids: Set[str] = set()
        empty_html_parse_ids: Set[str] = set()  # データなしだったrace_idを格納

        for i in range(0, len(races_to_process_info), RACE_BATCH_SIZE_FOR_PROCESSING):
            batch_race_infos = races_to_process_info[
                i : i + RACE_BATCH_SIZE_FOR_PROCESSING
            ]
            batch_race_ids_to_try = {
                r["race_id"] for r in batch_race_infos if "race_id" in r
            }

            if batch_race_ids_to_try:
                self.saver.update_race_step5_status_batch(
                    list(batch_race_ids_to_try), "processing_html"
                )

            futures = {}
            with ThreadPoolExecutor(
                max_workers=self.max_workers, thread_name_prefix="Step5Worker"
            ) as executor:
                for race_info_item in batch_race_infos:
                    if not race_info_item.get("race_id"):
                        self.logger.warning(
                            f"race_id 不明のためHTML処理スキップ: {race_info_item}"
                        )
                        continue
                    futures[
                        executor.submit(
                            self._fetch_and_parse_race_html_worker, race_info_item
                        )
                    ] = race_info_item["race_id"]

            for future in as_completed(futures):
                race_id_of_future = futures[future]
                try:
                    returned_race_id, parsed_data_dict = future.result()
                    if parsed_data_dict:
                        if parsed_data_dict.get("is_empty", False):
                            empty_html_parse_ids.add(returned_race_id)
                            self.logger.info(
                                f"Race ID {returned_race_id} はHTMLパース成功、ただしデータなしとして処理。"
                            )
                        else:
                            all_parsed_data_from_html.append(parsed_data_dict)
                            successful_html_parse_ids.add(returned_race_id)
                    else:  # parsed_data_dict が None の場合 (HTML取得失敗やパース中の予期せぬエラー)
                        failed_html_parse_ids.add(returned_race_id)
                        self.logger.warning(
                            f"Race ID {returned_race_id} のHTML取得またはパース処理でエラー発生。"
                        )
                except Exception as e_future:
                    self.logger.error(
                        f"Race ID {race_id_of_future} HTML処理ワーカーで予期せぬ例外 (as_completed): {e_future}",
                        exc_info=True,
                    )
                    failed_html_parse_ids.add(race_id_of_future)

            if (
                self.rate_limit_wait_html > 0
                and i + RACE_BATCH_SIZE_FOR_PROCESSING < len(races_to_process_info)
            ):
                self.logger.debug(f"バッチ処理間待機: {self.rate_limit_wait_html}秒")
                time.sleep(self.rate_limit_wait_html)

        total_attempted_html = len(successful_html_parse_ids) + len(
            failed_html_parse_ids
        )
        self.logger.info(
            f"HTML取得・パース完了。試行: {total_attempted_html}, 成功(データあり): {len(successful_html_parse_ids)}, データなし: {len(empty_html_parse_ids)}, 失敗: {len(failed_html_parse_ids)}"
        )

        if not all_parsed_data_from_html:
            self.logger.info("HTMLパースデータなし、保存スキップ。")
            # all_parsed_data_from_html が空でも empty_html_parse_ids は処理する必要がある
        # else: # all_parsed_data_from_html が空でも以下の処理は必要になる場合がある

        # データがあったものと、データがなかったもの、それぞれステータス更新の準備
        # race_ids_where_html_parsed は successful_html_parse_ids を使う
        if successful_html_parse_ids:
            self.saver.update_race_step5_status_batch(
                list(successful_html_parse_ids), "saving_html_data"
            )

        data_for_saver_race_results = []
        data_for_saver_inspection_reports = []
        data_for_saver_race_comments = []
        # lap_data_by_section は race_id ごとに辞書として渡す必要があるかもしれない
        # Saver側の save_lap_positions_batch の実装に依存する
        # ここでは、各レースの lap_data_by_section をリストに格納し、race_idも添える形を想定
        all_lap_data_for_saver: List[Dict[str, Any]] = []

        for (
            parsed_data_item
        ) in all_parsed_data_from_html:  # is_empty=False のものだけがここに来る
            race_id_current = parsed_data_item.get("race_id")
            if not race_id_current:
                self.logger.warning(
                    f"パース済みデータにrace_idがありません。スキップ: {parsed_data_item}"
                )
                continue

            if parsed_data_item.get("race_results"):
                data_for_saver_race_results.extend(parsed_data_item["race_results"])
            if parsed_data_item.get("inspection_reports"):
                data_for_saver_inspection_reports.extend(
                    parsed_data_item["inspection_reports"]
                )
            if parsed_data_item.get("race_comments"):
                data_for_saver_race_comments.extend(parsed_data_item["race_comments"])
            if parsed_data_item.get("lap_positions"):
                all_lap_data_for_saver.append(
                    {
                        "race_id": race_id_current,
                        "data": parsed_data_item["lap_positions"],
                    }
                )

            # race_meta は削除済みなので、updated_races_meta 関連の処理は不要

        save_errors_occurred_ids: Set[str] = set()
        try:
            # data_for_saver_race_results は使わずに、all_parsed_data_from_html をループする
            # if data_for_saver_race_results: # 変更前
            #     self.saver.save_race_results_batch(data_for_saver_race_results)

            for parsed_item in all_parsed_data_from_html:  # 変更後
                current_race_id = parsed_item.get("race_id")
                if not current_race_id:
                    continue

                race_results_to_save = parsed_item.get("race_results")
                if race_results_to_save:
                    save_success = self.saver.save_race_results_batch(
                        current_race_id, race_results_to_save
                    )
                    if not save_success:
                        save_errors_occurred_ids.add(current_race_id)

            # 他のデータ保存処理は変更なし (それぞれのSaverメソッドが race_id を含むデータを期待しているか確認が必要)
            # 現状、inspection_reports, race_comments, lap_positions は race_id を含む辞書のリストを期待しているように見える
            # もしそれらが race_id を別途引数で取るなら、ここも同様のループ処理が必要

            # inspection_reports の保存処理
            for parsed_item in all_parsed_data_from_html:
                current_race_id = parsed_item.get("race_id")
                inspection_reports_to_save = parsed_item.get("inspection_reports")
                if current_race_id and inspection_reports_to_save:
                    save_success = self.saver.save_inspection_reports_batch(
                        current_race_id, inspection_reports_to_save
                    )
                    if not save_success:
                        save_errors_occurred_ids.add(current_race_id)

            # レースコメントの保存
            if data_for_saver_race_comments:
                save_success_comments = self.saver.save_race_comments_batch(
                    current_race_id, data_for_saver_race_comments
                )
                if not save_success_comments:
                    self.logger.error(
                        f"Race ID {current_race_id}: レースコメントの保存に失敗しました。"
                    )
                    save_errors_occurred_ids.add(current_race_id)

            if (
                all_lap_data_for_saver
            ):  # lap_positionsは現状のままと仮定（Saver側の実装による）
                self.saver.save_lap_positions_batch(all_lap_data_for_saver)

            self.logger.info(
                f"{len(successful_html_parse_ids)}件のレースのHTML由来データ保存試行完了。"
            )
        except Exception as e_save:
            self.logger.error(f"Step5 Saver一括保存中にエラー: {e_save}", exc_info=True)
            # 保存エラーが発生した場合、どのrace_idでエラーになったか特定は難しい場合がある
            # ここでは、保存試行した successful_html_parse_ids 全てをエラー扱いとするか、
            # Saver側でエラーになったrace_idを返せるようにするか検討が必要。
            # 今回は、保存試行したものは一旦エラーとしてマークする。
            save_errors_occurred_ids.update(successful_html_parse_ids)

        # ステータス更新処理
        final_completed_ids = list(successful_html_parse_ids - save_errors_occurred_ids)
        final_error_ids = list(failed_html_parse_ids.union(save_errors_occurred_ids))
        final_data_not_available_ids = list(
            empty_html_parse_ids
        )  # is_empty=True だったもの

        if final_completed_ids:
            self.saver.update_race_step5_status_batch(final_completed_ids, "completed")
        if final_error_ids:
            self.saver.update_race_step5_status_batch(final_error_ids, "error")
        if final_data_not_available_ids:
            self.saver.update_race_step5_status_batch(
                final_data_not_available_ids, "data_not_available"
            )

        self.logger.info(
            f"Step5バルク更新完了。HTML試行: {total_attempted_html}, データあり成功: {len(successful_html_parse_ids)}, "
            f"データなし: {len(empty_html_parse_ids)}, HTML失敗: {len(failed_html_parse_ids)}, "
            f"最終完了: {len(final_completed_ids)}, 最終エラー: {len(final_error_ids)}, データなし確定: {len(final_data_not_available_ids)}"
        )

        return {
            "success": True,
            "message": "Step5 bulk update finished.",
            "details": {
                "total_races_identified": len(races_to_process_info),
                "html_fetch_parse_attempted": total_attempted_html,
                "html_fetch_parse_success": len(successful_html_parse_ids),
                "html_fetch_parse_failed": len(failed_html_parse_ids),
                "races_saved_successfully": len(final_completed_ids),
                "races_with_save_errors": len(save_errors_occurred_ids),
                "final_error_races": len(final_error_ids),
            },
        }


# --- 古いメソッド群はここより下には記述しない (削除する) ---
