# Yenjoy API

## 概要
Yenjoy（yen-joy.net）から競輪のレース結果や周回データなどの詳細情報を取得するためのスクレイピングガイドです。
このドキュメントは公式の競輪情報サイト「Yen-joy」からHTMLデータを解析する方法について説明しています。

## ベースURL
```
https://www.yen-joy.net
```

## 基本情報
- **形式**: HTML Webページ
- **レスポンス形式**: HTML（BeautifulSoupなどでのパースが必要）
- **機能**: レース結果、周回データ、選手情報の取得
- **認証**: 不要

## 主要ページ構造

| パス | 説明 | URLの例 |
|----------------|------|------------|
| `/kaisai/race/result/detail/{year_month}/{venue_code}/{start_date}/{race_date}/{race_number}` | レース結果詳細 | `/kaisai/race/result/detail/202502/45/20250221/20250223/12` |


## ページ構造とデータ抽出方法

### 1. レース結果詳細ページ

#### URL形式
```
/kaisai/race/result/detail/{year_month}/{venue_code}/{start_date}/{race_date}/{race_number}
```

#### パラメータ
| パラメータ | 型 | 説明 |
|------------|----|----|
| `year_month` | string | 年月（YYYYMM形式） |
| `venue_code` | string | 競輪場コード（例: `45` - 豊橋） |
| `start_date` | string | 開催初日（YYYYMMDD形式） |
| `race_date` | string | レース開催日（YYYYMMDD形式） |
| `race_number` | number | レース番号 |

#### 取得可能データ
BeautifulSoupでHTMLをパースすることで、以下の情報が取得できます：

- レース基本情報（グレード、天気、風速など）
- 着順情報（選手名、着順、車番、年齢、府県、期別、級班、着差、上り、決まり手など）
- 周回時の位置情報
- 払戻金情報
- オッズ情報
- 検車場レポート（選手コメント）

#### レース結果データ
レース結果テーブルは以下のような構造になっています：

```html
<table>
  <tr>
    <td>着</td>
    <td>車番</td>
    <td>印</td>
    <td>選手名</td>
    <td>年齢</td>
    <td>府県</td>
    <td>期別</td>
    <td>級班</td>
    <td>着差</td>
    <td>上り</td>
    <td>決まり手</td>
    <td>S/JH/B</td>
    <td>勝敗因</td>
    <td>個人状況</td>
    <td>S級 直近成績(現級)</td>
    <!-- その他のカラム -->
  </tr>
  <!-- 各選手の情報行 -->
  <tr>
    <td>1</td>
    <td>1</td>
    <td>○</td>
    <td>古性優作</td>
    <td>33</td>
    <td>大阪</td>
    <td>100</td>
    <td>SS</td>
    <td></td>
    <td>12.2</td>
    <td>追込み</td>
    <td>重注１４条第２項斜行走注１４条第２項斜行</td>
    <td>掬４角強襲</td>
    <td>118.38</td>
    <!-- 成績データ -->
  </tr>
  <!-- 他の選手のデータ -->
</table>
```

#### HTML要素の例

##### 検車場レポート
```html
<h3>検車場レポート</h3>
<table>
  <tr>
    <td>
      <img src="https://s3-ap-northeast-1.amazonaws.com/yenjoy/images/kaisai/2025/45/20250221/20250223/12/result-kensyajyou-report-9C-2529905.jpg" />
    </td>
    <td>古性 優作 大阪 100期</td>
  </tr>
</table>

<p>【古性 優作（1着）】<br>「（周回中は）いいところが取れたと思います。（深谷は）来るならここかなっていうところで来た。菅田（壱道）さんのけん制があって、前で菅田さんが消えた。外をう回するんだったら、前に踏んでそこで合ったところで勝負かなと。前（深谷と新山響平）はハイレベルな戦いだった。モニターを見て、仕掛けてくる選手を確認して、それでいった。僕が行けなくても、南さんが内から突き抜けてくれるかなと。今日（3日目）のアップでは、すごくいい感じだった。ある選手のローラーでピンときて、それをやってみた。いいところも悪いところもあったので、このあとローラーに乗ってそこをすり合わせていきたい」</p>

<p>【深谷 知広（2着）】<br>「（自分の仕掛けは）想定していたなかの1つでした。（後ろが）誰かっていう意識はなかったけど、アクシデントがあったのには気づいた。そこからは自分のペースでした。（番手に入った新山が仕掛けてきて）余力はなかったけど、気合が入って振り絞った。連日、声援をもらって、それがすごい力になっている」</p>

<p>【南 修二（3着）】<br>「（古性に）ちぎれないようにでした。（最後は）ミスしないように。（コースを）間違わないようにと。自分の状態はいいと思います。今日（3日目）が一番良かったんで、明日も大丈夫だと思います」</p>
```

##### 払戻金情報
```html
<div class="pay-block">
  <table>
    <thead>
      <tr>
        <th>勝式</th>
        <th>車番</th>
        <th>払戻金</th>
        <th>人気</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td rowspan="1">単勝</td>
        <td>1</td>
        <td>270円</td>
        <td>1</td>
      </tr>
      <tr>
        <td rowspan="2">複勝</td>
        <td>1</td>
        <td>130円</td>
        <td>1</td>
      </tr>
      <tr>
        <td>7</td>
        <td>170円</td>
        <td>3</td>
      </tr>
      <!-- その他の払戻情報 -->
    </tbody>
    <tfoot>
      <tr>
        <td colspan="4">発売票数 2,735,540票 発売金額 14,485,920円</td>
      </tr>
      <tr>
        <td colspan="11">古性が３番手確保し強襲。深谷は番手はまる新山合わせ逃げ粘る</td>
      </tr>
    </tfoot>
  </table>
</div>
```

##### レース概要情報
レース概要は通常ページのヘッダー部分に含まれています：
```html
<div class="race-data">
  <h4>2月23日(3日目) 第12R S級準決勝 天気:晴 風速:2.4m</h4>
  <!-- その他のレース情報 -->
</div>
```

## データ抽出のポイント

### レース結果の抽出

```python
def parse_race_results(self, soup):
    """
    レース結果テーブルから詳細情報を抽出
    """
    results = []
    
    # メインの結果テーブルを検索
    result_table = soup.find('table', {'id': 'race-result-table'})
    if not result_table:
        # IDがない場合、最初の大きなテーブルを探す
        result_table = soup.find('table', class_=lambda c: c and 'result' in c.lower())
    
    if not result_table:
        return results
    
    # 行を取得
    rows = result_table.find_all('tr')
    
    # ヘッダー行をスキップ（通常は1行目）
    for row in rows[1:]:
        cells = row.find_all('td')
        
        # 十分なセルがあるか確認
        if len(cells) < 10:
            continue
        
        # 落車などの特殊なステータスをチェック
        rank = cells[0].text.strip()
        
        result = {
            "rank": rank,  # 着順
            "frame_number": cells[1].text.strip(),  # 車番
            "mark": cells[2].text.strip() if len(cells) > 2 else "",  # 印
            "racer_name": cells[3].text.strip() if len(cells) > 3 else "",  # 選手名
            "age": cells[4].text.strip() if len(cells) > 4 else "",  # 年齢
            "prefecture": cells[5].text.strip() if len(cells) > 5 else "",  # 府県
            "period": cells[6].text.strip() if len(cells) > 6 else "",  # 期別
            "class": cells[7].text.strip() if len(cells) > 7 else "",  # 級班
            "diff": cells[8].text.strip() if len(cells) > 8 else "",  # 着差
            "last_lap": cells[9].text.strip() if len(cells) > 9 else "",  # 上り
            "winning_technique": cells[10].text.strip() if len(cells) > 10 else "",  # 決まり手
            "symbols": cells[11].text.strip() if len(cells) > 11 else "",  # S/JH/B
            "win_factor": cells[12].text.strip() if len(cells) > 12 else "",  # 勝敗因
            "personal_status": cells[13].text.strip() if len(cells) > 13 else "",  # 個人状況
        }
        
        # 成績データがあれば追加
        if len(cells) > 14:
            recent_results = []
            for i in range(14, min(len(cells), 24)):  # 直近10レースの成績
                recent_results.append(cells[i].text.strip())
            result["recent_results"] = recent_results
        
        results.append(result)
    
    return results
```

### 払戻金情報と発売票数の抽出

```python
def parse_payouts(self, soup):
    """
    払戻金情報とレース概要を抽出
    """
    payouts = {
        "win": [],
        "place": [],
        "quinella": [],
        "exacta": [],
        "trio": [],
        "trifecta": [],
        "race_info": {}
    }
    
    # 払戻テーブルを見つける
    payout_table = soup.find('div', class_='pay-block').find('table')
    if not payout_table:
        return payouts
    
    # tfootからレース概要情報を抽出
    tfoot = payout_table.find('tfoot')
    if tfoot:
        tfoot_rows = tfoot.find_all('tr')
        
        # 発売票数と発売金額
        if len(tfoot_rows) > 0:
            info_text = tfoot_rows[0].text.strip()
            # 例: "発売票数 2,735,540票 発売金額 14,485,920円"
            if "発売票数" in info_text:
                tickets_match = re.search(r'発売票数\s+([\d,]+)票', info_text)
                amount_match = re.search(r'発売金額\s+([\d,]+)円', info_text)
                
                if tickets_match:
                    payouts["race_info"]["tickets_count"] = tickets_match.group(1).replace(',', '')
                if amount_match:
                    payouts["race_info"]["total_amount"] = amount_match.group(1).replace(',', '')
        
        # レース展開の説明文
        if len(tfoot_rows) > 1:
            race_summary = tfoot_rows[1].text.strip()
            payouts["race_info"]["race_summary"] = race_summary
    
    # レース概要情報を追加取得（ヘッダー部分）
    race_header = soup.find('div', class_='race-data').find('h4')
    if race_header:
        header_text = race_header.text.strip()
        # 例: "2月23日(3日目) 第12R S級準決勝 天気:晴 風速:2.4m"
        day_match = re.search(r'(\d+月\d+日)\((\d+日目)\)', header_text)
        race_match = re.search(r'第(\d+)R\s+(.+?)\s+天気:(.+?)\s+風速:(.+?)(?:m|$)', header_text)
        
        if day_match:
            payouts["race_info"]["race_date"] = day_match.group(1)
            payouts["race_info"]["day_number"] = day_match.group(2)
        
        if race_match:
            payouts["race_info"]["race_number"] = race_match.group(1)
            payouts["race_info"]["race_class"] = race_match.group(2)
            payouts["race_info"]["weather"] = race_match.group(3)
            payouts["race_info"]["wind_speed"] = race_match.group(4)
    
    # 払戻金テーブルの各行を処理
    rows = payout_table.find('tbody').find_all('tr')
    current_bet_type = None
    
    for row in rows:
        cells = row.find_all('td')
        if not cells:
            continue
        
        # 勝式（単勝、複勝など）を取得
        if 'rowspan' in cells[0].attrs:
            current_bet_type = cells[0].text.strip()
        
        # 勝式によって処理分岐
        if current_bet_type == "単勝":
            payouts["win"].append({
                "numbers": cells[1].text.strip(),
                "amount": cells[2].text.strip().replace('円', '').replace(',', ''),
                "popularity": cells[3].text.strip()
            })
        elif current_bet_type == "複勝":
            payouts["place"].append({
                "numbers": cells[0].text.strip() if len(cells) == 3 else cells[1].text.strip(),
                "amount": cells[1 if len(cells) == 3 else 2].text.strip().replace('円', '').replace(',', ''),
                "popularity": cells[2 if len(cells) == 3 else 3].text.strip()
            })
        elif current_bet_type == "2連単":
            payouts["exacta"].append({
                "numbers": cells[1].text.strip(),
                "amount": cells[2].text.strip().replace('円', '').replace(',', ''),
                "popularity": cells[3].text.strip()
            })
        elif current_bet_type == "2連複":
            payouts["quinella"].append({
                "numbers": cells[1].text.strip(),
                "amount": cells[2].text.strip().replace('円', '').replace(',', ''),
                "popularity": cells[3].text.strip()
            })
        elif current_bet_type == "3連単":
            payouts["trifecta"].append({
                "numbers": cells[1].text.strip(),
                "amount": cells[2].text.strip().replace('円', '').replace(',', ''),
                "popularity": cells[3].text.strip()
            })
        elif current_bet_type == "3連複":
            payouts["trio"].append({
                "numbers": cells[1].text.strip(),
                "amount": cells[2].text.strip().replace('円', '').replace(',', ''),
                "popularity": cells[3].text.strip()
            })
    
    return payouts
```

### 検車場レポートの抽出

```python
def parse_inspection_report(self, soup):
    """
    検車場レポート情報を抽出
    """
    report_data = {}
    
    # 検車場レポートのセクションを探す
    report_heading = soup.find(string="検車場レポート")
    if not report_heading:
        return None
    
    # 検車場レポートを含む親要素を特定
    report_section = report_heading.find_parent('h3')
    if not report_section:
        return None
    
    # 選手写真のテーブルを取得
    photo_table = report_section.find_next('table')
    if photo_table:
    # 選手写真を取得
        photo_img = photo_table.find('img')
    if photo_img:
        report_data['racer_photo'] = photo_img.get('src', '')
        
        # 選手情報を取得
        info_cell = photo_table.find_all('td')
        if len(info_cell) > 1:
            report_data['racer_info'] = info_cell[1].text.strip()
    
    # コメント部分を取得
    comments = []
    comment_paragraphs = report_section.find_next_siblings('p')
    for p in comment_paragraphs:
        if p and p.text.strip():
            comments.append(p.text.strip())
    
    if comments:
        report_data['comments'] = comments
    
    return report_data
```

### 選手の周回位置情報の抽出

周回時の選手位置情報はレース結果ページから抽出できます。ただし、レーススピードについては明示的なデータがないため注意が必要です。

```python
def parse_lap_positions(self, soup):
    """
    選手の周回ごとの位置情報を抽出
    """
    positions_data = []
    
    # 選手の周回位置データを含むテーブルを探す
    # 注: 実際のサイトでの選手位置データの表示方法に応じて適切なセレクタを使用
    position_tables = soup.find_all('table', class_=lambda c: c and 'lap' in c.lower())
    
    for table in position_tables:
        # テーブルのヘッダーに「周回」や「位置」という文字があるか確認
        header = table.find('th')
        if not header or not any(keyword in header.text.lower() for keyword in ['周回', '位置']):
            continue
        
        # 各選手の行を処理
        for row in table.find_all('tr')[1:]:  # ヘッダー行をスキップ
            cells = row.find_all('td')
            if len(cells) < 2:  # 最低でも選手名と1周目の位置が必要
                continue
                
            # 最初のセルから選手情報を取得
            racer_info = cells[0].text.strip()
            
            # 周回ごとの位置データを収集
            lap_positions = []
            for i, cell in enumerate(cells[1:], 1):
                position = cell.text.strip()
                if position and position != '-':
                    try:
                        lap_positions.append({
                            'lap': i,
                            'position': int(position)
                        })
                    except ValueError:
                        # 数値に変換できない場合はスキップ
                        pass
            
            # 有効なデータがあれば追加
            if lap_positions:
                positions_data.append({
                    'racer_info': racer_info,
                    'positions': lap_positions
                })
    
    return positions_data
```

## 実装例

```python
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

class YenjoyAPI:
    """
    Yenjoyからデータを取得するスクレイピングクライアント
    
    HTMLをパースしてレース結果や周回データを取得
    """
    BASE_URL = "https://www.yen-joy.net"
    
    def __init__(self, logger=None):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        })
    
    def get_race_result(self, year_month, venue_code, start_date, race_date, race_number):
        """
        レース結果を取得
        
        Args:
            year_month (str): 年月 (YYYYMM形式)
            venue_code (str): 競輪場コード
            start_date (str): 開催初日 (YYYYMMDD形式)
            race_date (str): レース開催日 (YYYYMMDD形式)
            race_number (int): レース番号
            
        Returns:
            dict: レース結果情報
        """
        url = f"{self.BASE_URL}/kaisai/race/result/detail/{year_month}/{venue_code}/{start_date}/{race_date}/{race_number}"
        try:
        response = self.session.get(url)
            response.raise_for_status()  # HTTP エラーがあれば例外を発生
        soup = BeautifulSoup(response.text, 'html.parser')
        
            # HTMLから情報を抽出
            results = self.parse_race_results(soup)
            payouts = self.parse_payouts(soup)
            lap_positions = self.parse_lap_positions(soup)
            inspection_report = self.parse_inspection_report(soup)
        
        return {
                "url": url,
            "results": results,
            "payouts": payouts,
                "lap_positions": lap_positions,
                "inspection_report": inspection_report,
                "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
        except requests.exceptions.RequestException as e:
            if self.logger:
                self.logger.error(f"リクエストエラー: {e}")
            return {"error": str(e)}
        except Exception as e:
            if self.logger:
                self.logger.error(f"データ抽出エラー: {e}")
            return {"error": str(e)}
    
    def parse_race_results(self, soup):
        """
        レース結果テーブルから詳細情報を抽出
        """
        # 前述の実装を参照
        
    def parse_payouts(self, soup):
        """
        払戻金情報とレース概要を抽出
        """
        # 前述の実装を参照
        
    def parse_lap_positions(self, soup):
        """
        選手の周回ごとの位置情報を抽出
        """
        # 前述の実装を参照
        
    def parse_inspection_report(self, soup):
        """
        検車場レポート情報を抽出
        """
        # 前述の実装を参照
```

## 使用例

```python
import logging

# ロガーの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# APIクライアントの初期化
yenjoy = YenjoyAPI(logger=logger)

# レース結果の取得
# 例: 2025年2月23日の豊橋競輪 12レース
result = yenjoy.get_race_result(
    year_month="202502",    # 2025年2月
    venue_code="45",        # 豊橋競輪場
    start_date="20250221",  # 開催初日
    race_date="20250223",   # レース日
    race_number=12          # 12レース
)

# 結果の確認
if "error" not in result:
    print(f"レース情報を取得しました: {result['payouts']['race_info'].get('race_class', '')}")
    print(f"1着: {result['results'][0]['racer_name']} ({result['results'][0]['frame_number']}車)")
    print(f"単勝払戻金: {result['payouts']['win'][0]['amount']}円")
    
    # レース展開の表示
    if 'race_summary' in result['payouts']['race_info']:
        print(f"\nレース展開: {result['payouts']['race_info']['race_summary']}")
    
    # コメントの表示
    if result['inspection_report'] and 'comments' in result['inspection_report']:
        print("\n検車場レポート:")
        for comment in result['inspection_report']['comments']:
            print(comment[:100] + "..." if len(comment) > 100 else comment)
else:
    print(f"エラー: {result['error']}")
```

## スクレイピング利用上の注意点
1. サーバー負荷を考慮して、リクエスト間隔を適切に設けることをおすすめします（2秒以上）
2. 全てのページはHTMLで返されるため、適切なパース処理が必要です
3. サイトの構造変更に注意してください（HTML構造が変わるとパース処理が機能しなくなる可能性があります）
4. レースが終了していない場合など、データが存在しない場合があります
5. 過度なアクセスはサーバーに負荷をかけるためお控えください
6. 取得したデータの利用にあたっては、著作権やサイト利用規約を遵守してください