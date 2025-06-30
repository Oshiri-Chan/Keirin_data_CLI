# Winticket Cups API

## 概要
月間の競輪開催情報を取得するためのAPIエンドポイントです。

## エンドポイント
```
GET /cups
```

## パラメータ
### クエリパラメータ
| パラメータ | 型 | 必須 | 説明 |
|------------|----|----|------|
| `date` | string | はい | 取得する年月日（YYYYMMDD形式、月の1日を指定） |
| `fields` | string | いいえ | 取得するフィールド（"month,venues,regions"を指定） |
| `pfm` | string | いいえ | プラットフォーム（デフォルト: "web"） |

## レスポンス

### 成功時 (200 OK)
```json
{
  "month": {
    "cups": [
      {
        "id": "string",
        "name": "string",
        "startDate": "YYYY-MM-DD",
        "endDate": "YYYY-MM-DD",
        "duration": number,
        "grade": number,
        "venueId": "string",
        "labels": ["string"],
        "playersUnfixed": boolean
      }
    ],
    "regions": [
      {
        "id": "string",
        "name": "string"
      }
    ],
    "venues": [
      {
        "id": "string",
        "name": "string",
        "name1": "string",
        "slug": "string",
        "address": "string",
        "phoneNumber": "string",
        "regionId": "string",
        "websiteUrl": "string",
        "twitterAccountId": "string",
        "hasTwitterAccountId": boolean,
        "hasFood": boolean,
        "hasOfftrackVenues": boolean,
        "trackDistance": number,
        "trackStraightDistance": number,
        "trackAngleStraight": number,
        "trackAngleCenter": number,
        "homeWidth": number,
        "backWidth": number,
        "centerWidth": number,
        "bankFeature": "string",
        "bankImageUrl": "string",
        "bankPhotoUrl": "string",
        "exteriorPhotoUrl": "string",
        "description": "string",
        "bestRecord": {
          "playerId": "string",
          "second": number,
          "date": "YYYY-MM-DD",
          "updatedAt": number
        },
        "commemorationCup": {
          "name": "string",
          "season": "string"
        },
        "directions": [
          {
            "title": "string",
            "description": "string"
          }
        ],
        "factors": [
          {
            "title": "string",
            "datasets": [
              {
                "name": "string",
                "value": number
              }
            ]
          }
        ],
        "fees": [
          {
            "name": "string",
            "description": "string"
          }
        ],
        "food": {
          "text": "string",
          "images": ["string"]
        },
        "parkingLots": [
          {
            "title": "string",
            "description": "string"
          }
        ],
        "offtrackVenues": [
          {
            "name": "string",
            "address": "string"
          }
        ],
        "winnerHistories": [
          {
            "playerId": "string",
            "year": number
          }
        ]
      }
    ]
  }
}
```

### レスポンスフィールド
| フィールド | 型 | 説明 |
|------------|----|----|
| `month.cups` | array | 開催情報の配列 |
| `month.cups[].id` | string | 開催のユニークID（後続のAPIリクエストで使用） |
| `month.cups[].name` | string | 開催名（例: "高松宮記念杯", "平塚記念"） |
| `month.cups[].startDate` | string | 開催開始日（YYYY-MM-DD形式） |
| `month.cups[].endDate` | string | 開催終了日（YYYY-MM-DD形式） |
| `month.cups[].duration` | number | 開催日数 |
| `month.cups[].grade` | number | 開催のグレード（1: F2, 2: F1, 3: G3, 4: G2, 5: G1） |
| `month.cups[].venueId` | string | 競輪場ID |
| `month.cups[].labels` | array | 開催に関するラベル情報の配列 |
| `month.cups[].playersUnfixed` | boolean | 選手未確定フラグ |
| `month.regions` | array | 地域情報の配列 |
| `month.regions[].id` | string | 地域ID |
| `month.regions[].name` | string | 地域名 |
| `month.venues` | array | 競輪場情報の配列 |
| `month.venues[].id` | string | 競輪場ID |
| `month.venues[].name` | string | 競輪場名 |
| `month.venues[].name1` | string | 競輪場の短縮名 |
| `month.venues[].slug` | string | 競輪場のスラッグ（URL用） |
| `month.venues[].address` | string | 競輪場の住所 |
| `month.venues[].phoneNumber` | string | 競輪場の電話番号 |
| `month.venues[].regionId` | string | 所属地域ID |
| `month.venues[].websiteUrl` | string | 公式ウェブサイトURL |
| `month.venues[].twitterAccountId` | string | 公式TwitterアカウントID |
| `month.venues[].hasTwitterAccountId` | boolean | Twitter公式アカウント有無 |
| `month.venues[].hasFood` | boolean | 飲食施設の有無 |
| `month.venues[].hasOfftrackVenues` | boolean | 場外車券売り場の有無 |
| `month.venues[].trackDistance` | number | トラック1周の距離（メートル） |
| `month.venues[].trackStraightDistance` | number | 直線部分の距離（メートル） |
| `month.venues[].trackAngleStraight` | number | 直線角度（度） |
| `month.venues[].trackAngleCenter` | number | センター角度（度） |
| `month.venues[].homeWidth` | number | ホームストレッチの幅（メートル） |
| `month.venues[].backWidth` | number | バックストレッチの幅（メートル） |
| `month.venues[].centerWidth` | number | センター部分の幅（メートル） |
| `month.venues[].bankFeature` | string | バンク特性 |
| `month.venues[].bankImageUrl` | string | バンク画像URL |
| `month.venues[].bankPhotoUrl` | string | バンク写真URL |
| `month.venues[].exteriorPhotoUrl` | string | 外観写真URL |
| `month.venues[].description` | string | 競輪場の説明 |
| `month.venues[].bestRecord` | object | 最高記録情報 |
| `month.venues[].bestRecord.playerId` | string | 記録保持選手ID |
| `month.venues[].bestRecord.second` | number | 記録（秒） |
| `month.venues[].bestRecord.date` | string | 記録達成日 |
| `month.venues[].bestRecord.updatedAt` | number | 記録更新時刻 |
| `month.venues[].commemorationCup` | object | 記念競輪情報 |
| `month.venues[].commemorationCup.name` | string | 記念競輪名 |
| `month.venues[].commemorationCup.season` | string | 開催シーズン |
| `month.venues[].directions` | array | アクセス情報 |
| `month.venues[].directions[].title` | string | アクセス方法のタイトル |
| `month.venues[].directions[].description` | string | アクセス方法の説明 |
| `month.venues[].factors` | array | トラック特性情報 |
| `month.venues[].factors[].title` | string | 特性タイトル |
| `month.venues[].factors[].datasets` | array | 特性データセット |
| `month.venues[].factors[].datasets[].name` | string | データ名 |
| `month.venues[].factors[].datasets[].value` | number | データ値 |
| `month.venues[].fees` | array | 入場料情報 |
| `month.venues[].fees[].name` | string | 料金区分名 |
| `month.venues[].fees[].description` | string | 料金説明 |
| `month.venues[].food` | object | 飲食情報 |
| `month.venues[].food.text` | string | 飲食施設の説明 |
| `month.venues[].food.images` | array | 飲食施設の画像URL配列 |
| `month.venues[].parkingLots` | array | 駐車場情報 |
| `month.venues[].parkingLots[].title` | string | 駐車場名 |
| `month.venues[].parkingLots[].description` | string | 駐車場説明 |
| `month.venues[].offtrackVenues` | array | 場外車券売り場情報 |
| `month.venues[].offtrackVenues[].name` | string | 場外車券売り場名 |
| `month.venues[].offtrackVenues[].address` | string | 場外車券売り場住所 |
| `month.venues[].winnerHistories` | array | 過去の優勝者履歴 |
| `month.venues[].winnerHistories[].playerId` | string | 優勝選手ID |
| `month.venues[].winnerHistories[].year` | number | 優勝年 |

## サンプルリクエスト

### cURL
```bash
curl -X GET "https://api.winticket.jp/v1/keirin/cups?date=20250301&fields=month,venues,regions&pfm=web"
```

### Python
```python
import requests

url = "https://api.winticket.jp/v1/keirin/cups"
params = {
    "date": "20250301",
    "fields": "month,venues,regions",
    "pfm": "web"
}

response = requests.get(url, params=params)
data = response.json()

# 開催情報を取得
cups = data.get("month", {}).get("cups", [])
venues = {v["id"]: v for v in data.get("month", {}).get("venues", [])}

for cup in cups:
    venue_name = venues.get(cup["venueId"], {}).get("name", "不明")
    print(f"{cup['name']} ({venue_name}) {cup['startDate']} 〜 {cup['endDate']}")
    
    # 競輪場の基本情報を表示
    if cup["venueId"] in venues:
        venue = venues[cup["venueId"]]
        print(f"  住所: {venue.get('address')}")
        print(f"  バンク: {venue.get('trackDistance')}m")
```

## サンプルレスポンス
```json
{
  "month": {
    "cups": [
      {
        "id": "20250310FKMT",
        "name": "第1回F1福岡ミカン杯争奪戦",
        "startDate": "2025-03-10",
        "endDate": "2025-03-13",
        "duration": 4,
        "grade": 2,
        "venueId": "FKMT",
        "labels": ["F1", "ミッドナイト"],
        "playersUnfixed": false
      },
      {
        "id": "20250301MUKW",
        "name": "京王閣記念",
        "startDate": "2025-03-01",
        "endDate": "2025-03-06",
        "duration": 6,
        "grade": 5,
        "venueId": "MUKW",
        "labels": ["G1"],
        "playersUnfixed": false
      }
    ],
    "regions": [
      {
        "id": "KANTO",
        "name": "関東"
      },
      {
        "id": "KYUSHU",
        "name": "九州"
      }
    ],
    "venues": [
      {
        "id": "MUKW",
        "name": "京王閣",
        "name1": "京王閣",
        "address": "東京都調布市多摩川5-31-1",
        "phoneNumber": "042-487-3111",
        "regionId": "KANTO",
        "trackDistance": 400,
        "bankFeature": "両バンク加速型・直線攻め型",
        "bestRecord": {
          "playerId": "4719",
          "second": 40.5,
          "date": "2021-05-16",
          "updatedAt": 1621179600
        }
      },
      {
        "id": "FKMT",
        "name": "福岡",
        "name1": "福岡",
        "address": "福岡県福岡市中央区天神4-7-1",
        "phoneNumber": "092-761-2436",
        "regionId": "KYUSHU",
        "trackDistance": 500,
        "bankFeature": "両バンク加速型・バック攻め型",
        "bestRecord": {
          "playerId": "5057",
          "second": 43.2,
          "date": "2023-04-02",
          "updatedAt": 1680443400
        }
      }
    ]
  }
}
```

## エラーレスポンス

### 400 Bad Request
```json
{
  "error": {
    "code": "InvalidParameter",
    "message": "パラメータが不正です"
  }
}
```

### 404 Not Found
```json
{
  "error": {
    "code": "DataNotFound",
    "message": "データが見つかりませんでした"
  }
}
```

## 関連エンドポイント
- [開催詳細 API](./WinticketCupDetailAPI.md) - 特定の開催の詳細情報を取得
- [レース詳細 API](./WinticketRaceAPI.md) - レースの詳細情報を取得

## 注意事項
- 日付は月の1日を指定することで、その月の全開催情報が取得できます
- 実際の開催情報がない月の場合は空の配列が返されます
- 開催IDは後続のAPI呼び出しで必要となるため保存しておくことをお勧めします
- `fields`パラメータで取得したいセクションを指定することで、レスポンスサイズを最適化できます
- 競輪場情報（venues）には、バンク特性や交通アクセスなど詳細な施設情報が含まれています 