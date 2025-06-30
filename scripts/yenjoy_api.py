import logging
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from api_rate_limiter import ApiBackoff, ApiRateLimiter

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("yenjoy_api.log"), logging.StreamHandler()],
)
logger = logging.getLogger("YenjoyAPI")


class YenjoyAPI:
    """
    Yen-joy.netから競輪の結果データを取得するAPIクラス
    """

    def __init__(self, config_file=None, winticket_api=None):
        """
        Yen-joy.netから競輪の結果データを取得するAPIクラス
        """
        self.base_url = "https://www.yen-joy.net"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Referer": "https://www.yen-joy.net/",
            "Connection": "keep-alive",
        }
        self.debug = True
        self.no_images = True

        # デフォルト設定
        default_rate_limit = 2.0
        calendar_rate_limit = 3.0
        result_rate_limit = 2.0
        initial_delay = 2.0
        max_retries = 3

        # 設定を読み込み
        if config_file:
            try:
                from keirin_config import get_config

                self.config = get_config(config_file)
                yenjoy_config = self.config.get_yenjoy_config()

                # 設定から値を取得
                default_rate_limit = yenjoy_config["default_rate_limit"]
                calendar_rate_limit = yenjoy_config["calendar_rate_limit"]
                result_rate_limit = yenjoy_config["result_rate_limit"]
                initial_delay = yenjoy_config["initial_delay"]
                max_retries = yenjoy_config["max_retries"]
            except Exception as e:
                logger.warning(f"設定ファイル読み込みエラー: {e}")
                logger.warning("デフォルト値を使用します")

        # APIレート制限設定
        self.rate_limiter = ApiRateLimiter(default_rate_limit=default_rate_limit)
        self.rate_limiter.set_rate_limit("calendar", calendar_rate_limit)
        self.rate_limiter.set_rate_limit("result", result_rate_limit)

        # バックオフ設定
        self.backoff = ApiBackoff(initial_delay=initial_delay, max_retries=max_retries)

        # WinticketAPIインスタンスを保持
        self.winticket_api = winticket_api

        logger.info("YenjoyAPIインスタンスを初期化しました")

    def _make_request(self, url, endpoint=None, params=None):
        """
        HTTPリクエストを送信して、応答をBeautifulSoupオブジェクトとして返す

        Args:
            url (str): リクエストURL
            endpoint (str, optional): APIエンドポイント名（レート制限用）
            params (dict, optional): クエリパラメータ

        Returns:
            BeautifulSoup: 解析済みのHTMLデータ
        """

        def request_func():
            response = requests.get(
                url, headers=self.headers, params=params, timeout=30
            )
            response.raise_for_status()

            # BeautifulSoupで解析
            if self.no_images:
                # img要素を削除してから解析（メモリ使用量削減）
                html = re.sub(r"<img\s+[^>]*>", "", response.text)
                soup = BeautifulSoup(html, "html.parser")
            else:
                soup = BeautifulSoup(response.text, "html.parser")

            return soup

        # エンドポイントが指定されていない場合はデフォルト値を設定
        if endpoint is None:
            endpoint = "default"

        # バックオフ戦略と併用してレート制限を適用
        try:
            # まずレート制限を適用
            self.rate_limiter.wait(endpoint)
            # 次にバックオフ戦略を適用した実行
            return self.backoff.execute_with_retry(request_func, endpoint)
        except Exception as e:
            logger.error(f"リクエストエラー ({url}): {str(e)}")
            raise

    def get_kaisai_list(self, date_str):
        """
        指定日の開催一覧を取得

        Args:
            date_str (str): 日付 (YYYYMMDD形式)

        Returns:
            list: 開催情報のリスト
        """
        url = f"{self.base_url}/kaisai/day/{date_str}"

        try:
            response = self._make_request(url)
            soup = self._parse_html(response.text)

            kaisai_list = []

            # 開催一覧を取得
            kaisai_elements = soup.select(".kaisaiList li")
            for element in kaisai_elements:
                kaisai_info = {}

                # 開催場所の取得
                place_elem = element.select_one(".kaisaiPlace")
                if place_elem:
                    kaisai_info["place"] = place_elem.text.strip()

                # 開催IDの取得
                link_elem = element.select_one("a")
                if link_elem and "href" in link_elem.attrs:
                    href = link_elem.attrs["href"]
                    match = re.search(r"/kaisai/race/index/(\d+)/(\d+)/(\d+)", href)
                    if match:
                        kaisai_info["kaisai_id"] = match.group(1)
                        kaisai_info["day"] = match.group(2)
                        kaisai_info["date"] = match.group(3)

                # 開催名の取得
                name_elem = element.select_one(".kaisaiName")
                if name_elem:
                    kaisai_info["name"] = name_elem.text.strip()

                # 有効なデータが取得できた場合のみリストに追加
                if "kaisai_id" in kaisai_info:
                    kaisai_list.append(kaisai_info)

            logger.info(f"{date_str}の開催一覧を取得: {len(kaisai_list)}件")
            return kaisai_list

        except Exception as e:
            logger.error(f"開催一覧取得エラー: {e}")
            return []

    def get_race_list(self, kaisai_id, day, date_str):
        """
        指定開催のレース一覧を取得

        Args:
            kaisai_id (str): 開催ID
            day (str): 開催日数
            date_str (str): 日付 (YYYYMMDD形式)

        Returns:
            list: レース情報のリスト
        """
        url = f"{self.base_url}/kaisai/race/index/{kaisai_id}/{day}/{date_str}"

        try:
            response = self._make_request(url)
            soup = self._parse_html(response.text)

            race_list = []

            # レース一覧を取得
            race_elements = soup.select(".raceList li")
            for element in race_elements:
                race_info = {"kaisai_id": kaisai_id, "day": day, "date": date_str}

                # レース番号の取得
                race_num_elem = element.select_one(".raceNum")
                if race_num_elem:
                    race_num_text = race_num_elem.text.strip()
                    match = re.search(r"(\d+)R", race_num_text)
                    if match:
                        race_info["race_number"] = int(match.group(1))

                # レース名の取得
                race_name_elem = element.select_one(".raceName")
                if race_name_elem:
                    race_info["race_name"] = race_name_elem.text.strip()

                # レース結果へのリンク取得
                link_elem = element.select_one("a")
                if link_elem and "href" in link_elem.attrs:
                    href = link_elem.attrs["href"]
                    race_info["result_link"] = href

                # レース状態の取得
                status_elem = element.select_one(".status")
                if status_elem:
                    status_text = status_elem.text.strip()
                    race_info["status"] = status_text

                # 有効なデータが取得できた場合のみリストに追加
                if "race_number" in race_info:
                    race_list.append(race_info)

            logger.info(
                f"開催ID {kaisai_id} 第{day}日 ({date_str}) のレース一覧を取得: {len(race_list)}件"
            )
            return race_list

        except Exception as e:
            logger.error(f"レース一覧取得エラー: {e}")
            return []

    def get_race_result(self, result_link):
        """
        レース結果ページからデータを抽出

        Args:
            result_link (str): 結果ページのURL

        Returns:
            dict: 抽出された結果データ
        """
        if not result_link.startswith("http"):
            if result_link.startswith("/"):
                result_link = f"{self.base_url}{result_link}"
            else:
                result_link = f"{self.base_url}/{result_link}"

        try:
            # 結果ページを取得
            logger.info(f"レース結果ページを取得中: {result_link}")
            soup = self._make_request(result_link, endpoint="result")

            result_data = {
                "race_info": {},
                "result_list": [],
                "payout_info": [],
                "lap_info": {},  # 周回情報を追加
            }

            # レース情報の取得
            race_info_elem = soup.select_one(".raceInfo")
            if race_info_elem:
                # レース名
                race_name_elem = race_info_elem.select_one(".raceName")
                if race_name_elem:
                    result_data["race_info"]["race_name"] = race_name_elem.text.strip()

                # レース番号
                race_num_elem = race_info_elem.select_one(".raceNum")
                if race_num_elem:
                    race_num_text = race_num_elem.text.strip()
                    match = re.search(r"(\d+)R", race_num_text)
                    if match:
                        result_data["race_info"]["race_number"] = int(match.group(1))

                # 開催場所
                place_elem = race_info_elem.select_one(".place")
                if place_elem:
                    result_data["race_info"]["place"] = place_elem.text.strip()

                # レース情報（天候、走路状況など）
                race_condition_elem = race_info_elem.select_one(".raceCondition")
                if race_condition_elem:
                    condition_text = race_condition_elem.text.strip()
                    # 天気と風速を抽出
                    weather_match = re.search(r"天気:([^ ]+)", condition_text)
                    wind_match = re.search(r"風速:([^ ]+)", condition_text)

                    if weather_match:
                        result_data["race_info"]["weather"] = weather_match.group(1)
                    if wind_match:
                        result_data["race_info"]["wind_speed"] = wind_match.group(1)

                    result_data["race_info"]["condition"] = condition_text

            # 着順データの取得（result-table-detailクラス）
            result_table = soup.select_one(".result-table-detail")
            if result_table:
                # テーブルの各行を取得
                rows = result_table.select("tbody tr")

                # ヘッダー行があれば取得（列の意味を把握するため）
                headers = []
                header_row = result_table.select_one("thead tr")
                if header_row:
                    headers = [th.text.strip() for th in header_row.select("th")]

                # データ行の処理
                for row in rows:
                    cells = row.select("td")
                    if len(cells) >= 5:  # 最低限必要なセル数
                        result_item = {}

                        # 各セルの意味をヘッダーから判断
                        for i, cell in enumerate(cells):
                            if i < len(headers):
                                header = headers[i]
                                # キー名を英語に変換
                                key = self._convert_header_to_key(header)
                                if key:
                                    result_item[key] = cell.text.strip()

                            # ヘッダーがない場合や列数が合わない場合の対応
                            if len(headers) == 0 or i >= len(headers):
                                # 着順は通常1列目
                                if i == 0:
                                    result_item["rank"] = cell.text.strip()
                                # 車番は通常2列目
                                elif i == 1:
                                    result_item["number"] = cell.text.strip()
                                # 選手名は通常3列目
                                elif i == 2:
                                    result_item["player_name"] = cell.text.strip()

                        # 選手IDの取得（選手名のリンクからIDを抽出）
                        player_link = row.select_one('a[href*="/racer/data/"]')
                        if player_link and "href" in player_link.attrs:
                            player_id_match = re.search(
                                r"/racer/data/(\d+)", player_link.attrs["href"]
                            )
                            if player_id_match:
                                result_item["player_id"] = player_id_match.group(1)

                        # 有効なデータのみ追加
                        if "rank" in result_item and "number" in result_item:
                            result_data["result_list"].append(result_item)

            # 着順データがない場合、代替テーブルを探す
            if not result_data["result_list"]:
                # 全テーブルを検索して着順データらしいものを探す
                for table in soup.select("table"):
                    table_text = table.text.lower()
                    # 着順データらしい文字列が含まれているか確認
                    if any(key in table_text for key in ["着", "選手名", "車番"]):
                        rows = table.select("tr")
                        for row in rows:
                            cells = row.select("td")
                            if len(cells) >= 3:
                                result_item = {}

                                # 簡易的な着順情報の抽出
                                if len(cells) > 0:
                                    result_item["rank"] = cells[0].text.strip()
                                if len(cells) > 1:
                                    result_item["number"] = cells[1].text.strip()
                                if len(cells) > 2:
                                    result_item["player_name"] = cells[2].text.strip()

                                # 有効なデータのみ追加
                                if "rank" in result_item and "number" in result_item:
                                    result_data["result_list"].append(result_item)

                        # 一つのテーブルから抽出したら終了
                        if result_data["result_list"]:
                            break

            # 払戻情報の取得（result-payクラス）
            payout_table = soup.select_one(".result-pay")
            if payout_table:
                # テーブル内の行を処理
                rows = payout_table.select("tr")

                for row in rows:
                    # td要素が3つ以上ある行のみ処理
                    cells = row.select("td")
                    if len(cells) >= 3:
                        ticket_type = None
                        for i, cell in enumerate(cells):
                            # 券種（最初のセル）
                            if i == 0:
                                ticket_type = cell.text.strip()
                                if ticket_type and "未発売" not in ticket_type:
                                    if "複" in ticket_type:
                                        payout_type = (
                                            "複勝" if "車" in ticket_type else "複勝"
                                        )
                                    elif "単" in ticket_type:
                                        payout_type = (
                                            "単勝" if "車" in ticket_type else "単勝"
                                        )
                                    else:
                                        payout_type = ticket_type

                                    # 払戻情報を初期化
                                    payout_item = {
                                        "ticket_type": payout_type,
                                        "combination": "",
                                        "amount": 0,
                                        "popularity": 0,
                                    }

                            # 組合せと払戻金額（2列目以降）
                            elif i >= 1 and ticket_type and "未発売" not in ticket_type:
                                cell_text = cell.text.strip()

                                # 組合せ（例: "1 = 4 130円 (1)"）を解析
                                combo_match = re.search(
                                    r"([0-9=\s]+)\s*(\d+)円\s*\((\d+)\)", cell_text
                                )
                                if combo_match:
                                    combo = combo_match.group(1).strip()
                                    amount = int(combo_match.group(2))
                                    popularity = int(combo_match.group(3))

                                    # 新しい払戻項目を作成
                                    payout_item = {
                                        "ticket_type": payout_type,
                                        "combination": combo,
                                        "amount": amount,
                                        "popularity": popularity,
                                    }

                                    # 組合せから車番を抽出
                                    numbers = re.findall(r"\d+", combo)
                                    if len(numbers) >= 1:
                                        payout_item["number1"] = int(numbers[0])
                                    if len(numbers) >= 2:
                                        payout_item["number2"] = int(numbers[1])
                                    if len(numbers) >= 3:
                                        payout_item["number3"] = int(numbers[2])

                                    # 有効なデータのみ追加
                                    if payout_item["amount"] > 0:
                                        result_data["payout_info"].append(payout_item)

            # 払戻情報がない場合、代替テーブルを探す
            if not result_data["payout_info"]:
                for table in soup.select("table"):
                    table_text = table.text.lower()
                    # 払戻情報らしい文字列が含まれているか確認
                    if any(key in table_text for key in ["払戻", "円", "ワイド"]):
                        rows = table.select("tr")
                        for row in rows:
                            cells = row.select("td")
                            cell_text = " ".join([cell.text.strip() for cell in cells])

                            # ワイド情報のパターン: "ワイド 1 = 2 380円 (7) 1 = 4 130円 (1) 2 = 4 240円 (3)"
                            wide_patterns = re.findall(
                                r"(\d+)\s*=\s*(\d+)\s*(\d+)円\s*\((\d+)\)", cell_text
                            )
                            for pattern in wide_patterns:
                                if len(pattern) >= 4:
                                    num1, num2, amount, popularity = pattern
                                    payout_item = {
                                        "ticket_type": "ワイド",
                                        "combination": f"{num1} = {num2}",
                                        "number1": int(num1),
                                        "number2": int(num2),
                                        "amount": int(amount),
                                        "popularity": int(popularity),
                                    }
                                    result_data["payout_info"].append(payout_item)

                            # 2連単、3連単などのパターン
                            other_patterns = re.findall(
                                r"([^\s]+)\s+([0-9\s\-]+)\s+(\d+)円\s*\((\d+)\)",
                                cell_text,
                            )
                            for pattern in other_patterns:
                                if len(pattern) >= 4:
                                    ticket, combo, amount, popularity = pattern
                                    # 券種を判断
                                    if "2" in ticket and "単" in ticket:
                                        ticket_type = "2車単"
                                    elif "3" in ticket and "単" in ticket:
                                        ticket_type = "3連単"
                                    elif "2" in ticket and "複" in ticket:
                                        ticket_type = "2車複"
                                    elif "3" in ticket and "複" in ticket:
                                        ticket_type = "3連複"
                                    else:
                                        ticket_type = ticket

                                    payout_item = {
                                        "ticket_type": ticket_type,
                                        "combination": combo,
                                        "amount": int(amount),
                                        "popularity": int(popularity),
                                    }

                                    # 組合せから車番を抽出
                                    numbers = re.findall(r"\d+", combo)
                                    if len(numbers) >= 1:
                                        payout_item["number1"] = int(numbers[0])
                                    if len(numbers) >= 2:
                                        payout_item["number2"] = int(numbers[1])
                                    if len(numbers) >= 3:
                                        payout_item["number3"] = int(numbers[2])

                                    # 有効なデータのみ追加
                                    if payout_item["amount"] > 0:
                                        result_data["payout_info"].append(payout_item)

            # 周回情報の取得
            lap_info = self._extract_lap_info(soup)
            if lap_info:
                result_data["lap_info"] = lap_info

            logger.info(
                f"レース結果を取得: {result_data['race_info'].get('race_name', 'Unknown')}"
            )
            return result_data

        except Exception as e:
            logger.error(f"レース結果取得エラー: {e}")
            return None

    def _convert_header_to_key(self, header):
        """
        日本語のヘッダーを英語のキーに変換

        Args:
            header (str): 日本語のヘッダー文字列

        Returns:
            str: 英語のキー名、対応するものがなければNone
        """
        # ヘッダーと英語キーのマッピング
        header_map = {
            "着": "rank",
            "車番": "number",
            "選手名": "player_name",
            "年齢": "age",
            "府県": "prefecture",
            "期別": "term",
            "級班": "class",
            "着差": "diff",
            "上り": "time",
            "決まり手": "decisive",
        }

        # 完全一致で検索
        if header in header_map:
            return header_map[header]

        # 部分一致で検索
        for jp, en in header_map.items():
            if jp in header:
                return en

        # 対応するものがなければNone
        return None

    def _extract_lap_info(self, soup):
        """
        周回情報を抽出する

        Args:
            soup (BeautifulSoup): 解析済みのHTMLデータ

        Returns:
            dict: セクション名をキーとした周回情報
        """
        try:
            # 周回情報が含まれる部分を特定
            lap_wrapper = soup.find("div", class_="result-b-hyo-lap-wrapper")
            if not lap_wrapper:
                return {}

            # セクションごとのデータを保持する辞書
            lap_data = {}

            # 各セクション（<div class="b-hyo">）をループ処理
            sections = lap_wrapper.find_all("div", class_="b-hyo")
            for section_div in sections:
                # セクション名は<th class="bg-base-color">内のテキスト
                th = section_div.find("th", class_="bg-base-color")
                if th is None:
                    continue
                section_name = "".join(th.stripped_strings)

                # セクションごとのレコードリストを初期化
                records = []

                # セクション内の各選手データは<span class="bike-icon-wrapper">にある
                span_list = section_div.find_all("span", class_="bike-icon-wrapper")
                for span in span_list:
                    # spanのclass属性から車番（bikeno-○）、x-○、y-○の値を抽出
                    classes = span.get("class", [])
                    bike_no = None
                    x_val = None
                    y_val = None
                    for cls in classes:
                        if cls.startswith("bikeno-"):
                            bike_no = cls.split("-")[1]
                        elif cls.startswith("x-"):
                            x_val = int(cls.split("-")[1])
                        elif cls.startswith("y-"):
                            y_val = int(cls.split("-")[1])

                    # 選手名は、子要素<span class="racer-nm">のテキストから取得
                    racer_span = span.find("span", class_="racer-nm")
                    racer_name = racer_span.get_text(strip=True) if racer_span else ""

                    # 辞書形式でレコードを追加
                    records.append(
                        {"車番": bike_no, "選手名": racer_name, "x": x_val, "y": y_val}
                    )

                # セクション名をキーにしてデータを保持
                lap_data[section_name] = records

            return lap_data

        except Exception as e:
            logger.error(f"周回情報抽出エラー: {e}")
            return {}

    def get_race_result_by_params(
        self, year_month, keirin_code, first_day, kaisai_day, race_number
    ):
        """
        パラメータを指定してレース結果を直接取得

        Args:
            year_month (str): 年月 (YYYYMM形式)
            keirin_code (str): 競輪場コード
            first_day (str): 開催初日 (YYYYMMDD形式)
            kaisai_day (str): 当日の日付 (YYYYMMDD形式)
            race_number (str or int): レース番号

        Returns:
            dict: レース結果データ
        """
        url = f"{self.base_url}/kaisai/race/result/detail/{year_month}/{keirin_code}/{first_day}/{kaisai_day}/{race_number}"
        logger.info(f"直接URLでレース結果を取得: {url}")
        return self.get_race_result(url)

    def is_result_available(self, date_str):
        """
        指定された日付のレース結果が利用可能かどうかを確認

        Args:
            date_str (str): 日付 (YYYYMMDD形式)

        Returns:
            bool: 結果が利用可能な場合はTrue
        """
        try:
            # カレンダーページから確認
            url = f"{self.base_url}/racelist/{date_str[:6]}/index.html"
            soup = self._make_request(url, endpoint="calendar")

            # 指定日の結果へのリンクがあるか確認
            date_links = soup.select(f'a[href*="{date_str}"]')

            if not date_links:
                logger.info(
                    f"{date_str}のレース結果リンクがカレンダーに見つかりませんでした"
                )
                return False

            # 結果ページへのリンクがあるか確認
            for link in date_links:
                href = link.get("href", "")
                if "result" in href:
                    logger.info(f"{date_str}のレース結果が利用可能です")
                    return True

            logger.info(f"{date_str}の結果ページへのリンクが見つかりませんでした")
            return False

        except Exception as e:
            logger.error(f"結果利用可能性確認エラー: {str(e)}")
            return False

    def convert_result_to_df(self, result_data):
        """
        レース結果データをDataFrameに変換

        Args:
            result_data (dict): レース結果データ

        Returns:
            tuple: (結果DataFrame, 払戻DataFrame, 周回情報DataFrame)
        """
        if not result_data:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # レース結果をDataFrameに変換
        result_df = pd.DataFrame(result_data["result_list"])

        # 払戻情報をDataFrameに変換
        payout_df = pd.DataFrame(result_data["payout_info"])

        # 周回情報をDataFrameに変換
        lap_info = result_data.get("lap_info", {})
        lap_df = pd.DataFrame()

        if lap_info:
            # 各セクションのレコードを文字列リストに変換し、最大件数に合わせてNoneで埋める
            df_dict = {}
            max_length = (
                max(len(records) for records in lap_info.values()) if lap_info else 0
            )

            for section, records in lap_info.items():
                # 各レコードを文字列に変換
                values = []
                for rec in records:
                    values.append(
                        f"車番:{rec['車番']} 選手名:{rec['選手名']} (x:{rec['x']}, y:{rec['y']})"
                    )

                # 最大長に合わせてNoneで埋める
                if len(values) < max_length:
                    values.extend([None] * (max_length - len(values)))
                df_dict[section] = values

            # DataFrameに変換
            if df_dict:
                lap_df = pd.DataFrame(df_dict)

        # レース情報を追加
        if not result_df.empty:
            for key, value in result_data["race_info"].items():
                result_df[key] = value

        if not payout_df.empty:
            for key, value in result_data["race_info"].items():
                payout_df[key] = value

        return result_df, payout_df, lap_df

    def get_results_for_date(self, date_str):
        """
        指定日のすべてのレース結果をDataFrameとして取得

        Args:
            date_str (str): 日付 (YYYYMMDD形式)

        Returns:
            tuple: (結果DataFrame, 払戻DataFrame, 周回情報DataFrame)
        """
        try:
            # カレンダーページからその日のレース一覧を取得
            calendar_url = f"{self.base_url}/racelist/{date_str[:6]}/index.html"
            soup = self._make_request(calendar_url, endpoint="calendar")

            # 指定日の結果ページリンクを探す
            date_links = []
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if date_str in href and "result" in href:
                    date_links.append(href)

            if not date_links:
                logger.warning(f"{date_str}のレース結果リンクが見つかりませんでした")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

            # 各レースの結果を取得して結合
            all_results = []
            all_payouts = []
            all_laps = []

            for result_link in date_links:
                try:
                    # 絶対URLに変換
                    if not result_link.startswith("http"):
                        if result_link.startswith("/"):
                            full_link = f"{self.base_url}{result_link}"
                        else:
                            full_link = f"{self.base_url}/{result_link}"
                    else:
                        full_link = result_link

                    logger.info(f"レース結果を取得中: {full_link}")

                    # レース結果を取得
                    result_data = self.backoff.execute_with_retry(
                        self.get_race_result, "result", full_link
                    )

                    if not result_data:
                        logger.warning(f"レース結果データがありません: {full_link}")
                        continue

                    # データフレームに変換
                    result_df, payout_df, lap_df = self.convert_result_to_df(
                        result_data
                    )

                    if not result_df.empty:
                        all_results.append(result_df)
                    if not payout_df.empty:
                        all_payouts.append(payout_df)
                    if not lap_df.empty:
                        all_laps.append(lap_df)

                except Exception as e:
                    logger.error(f"レース結果取得エラー ({result_link}): {str(e)}")
                    continue

            # 全結果を結合
            combined_results = pd.DataFrame()
            combined_payouts = pd.DataFrame()
            combined_laps = pd.DataFrame()

            if all_results:
                combined_results = pd.concat(all_results, ignore_index=True)
            if all_payouts:
                combined_payouts = pd.concat(all_payouts, ignore_index=True)
            if all_laps:
                combined_laps = pd.concat(all_laps, ignore_index=True)

            logger.info(f"{date_str}のレース結果を取得完了: {len(combined_results)}件")
            return combined_results, combined_payouts, combined_laps

        except Exception as e:
            logger.error(f"結果取得エラー ({date_str}): {str(e)}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def get_result_by_direct_url(
        self, year_month, venue_code, first_day, target_date, race_number
    ):
        """
        URLを直接指定してレース結果を取得

        Args:
            year_month (str): 年月 (YYYYMM形式)
            venue_code (str): 競輪場コード
            first_day (str): 開催初日 (YYYYMMDD形式)
            target_date (str): 対象日 (YYYYMMDD形式)
            race_number (int): レース番号

        Returns:
            tuple: (結果DataFrame, 払戻DataFrame, 周回情報DataFrame)
        """
        try:
            # URLを構築
            url = f"{self.base_url}/racelist/{year_month}/{venue_code}{first_day}/result/{target_date}_{race_number}.html"
            logger.info(f"直接URLでレース結果を取得中: {url}")

            # レート制限とバックオフを適用してデータを取得
            def get_result_data():
                try:
                    soup = self._make_request(url, endpoint="direct_result")
                    result_data = self._extract_race_result(soup)
                    return result_data
                except Exception as e:
                    logger.error(f"レース結果取得エラー ({url}): {str(e)}")
                    raise

            # バックオフ戦略を適用して取得
            result_data = self.backoff.execute_with_retry(
                get_result_data, endpoint="direct_result"
            )

            if not result_data:
                logger.warning(f"直接URLでデータを取得できませんでした: {url}")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

            # DataFrameに変換
            result_df, payout_df, lap_df = self.convert_result_to_df(result_data)
            logger.info(f"直接URLでレース結果取得成功: {url}")
            return result_df, payout_df, lap_df

        except Exception as e:
            logger.error(f"直接URL取得エラー: {str(e)}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# 使用例
if __name__ == "__main__":
    # WinticketAPIインスタンスを先に作成
    from winticket_api import WinticketAPI

    winticket_api = WinticketAPI()
    print("WinticketAPIインスタンスを初期化しました")

    # WinticketAPIを渡してYenjoyAPIを初期化
    api = YenjoyAPI(winticket_api=winticket_api)

    # テスト用の日付
    date_str = datetime.now().strftime("%Y%m%d")

    # 開催情報の取得
    kaisai_list = api.get_kaisai_list(date_str)
    print(f"開催一覧: {kaisai_list}")

    # 結果が利用可能か確認
    is_available = api.is_result_available(date_str)
    print(f"結果利用可能: {is_available}")

    if is_available and kaisai_list:
        # 最初の開催のレース一覧を取得
        kaisai = kaisai_list[0]
        race_list = api.get_race_list(
            kaisai["kaisai_id"], kaisai.get("day", "1"), date_str
        )
        print(f"レース一覧: {race_list}")

        # 確定したレースの結果を取得
        for race in race_list:
            if race.get("status") == "確定" and race.get("result_link"):
                result = api.get_race_result(race["result_link"])
                print(f"レース結果: {result}")
                break

    # 直接URLからの結果取得テスト
    # 例: 2025年3月15日の玉野競輪（コード61）の第12レース
    result_df, payout_df, lap_df = api.get_result_by_direct_url(
        "202503", "61", "20250314", "20250315", "12"
    )
    if not result_df.empty:
        print(f"直接URL結果取得: {len(result_df)}件のレース結果")
        print(result_df.head())

        print(f"払戻情報: {len(payout_df)}件")
        print(payout_df.head())

        print("周回情報:")
        print(lap_df.head())
