# Winticket Cup Detail API

## 概要
特定の競輪開催の詳細情報を取得するためのAPIエンドポイントです。

## エンドポイント
```
GET /cups/{cup_id}
```

## パラメータ
### パスパラメータ
| パラメータ | 型 | 必須 | 説明 |
|------------|----|----|------|
| `cup_id` | string | はい | 開催ID（例: `2025031813`） |

### クエリパラメータ
| パラメータ | 型 | 必須 | 説明 |
|------------|----|----|------|
| `fields` | string | いいえ | 取得するフィールド（"cup,schedules,races"などカンマ区切りで指定） |
| `pfm` | string | いいえ | プラットフォーム（デフォルト: "web"） |

## レスポンス

### 成功時 (200 OK)
```json
{
  "cup": {
    "id": "string",
    "name": "string",
    "startDate": "YYYY-MM-DD",
    "endDate": "YYYY-MM-DD",
    "duration": number,
    "grade": number,
    "venueId": "string",
    "labels": ["string"],
    "playersUnfixed": boolean
  },
  "schedules": [
    {
      "id": "string",
      "date": "YYYY-MM-DD",
      "day": number,
      "cupId": "string",
      "index": number,
      "entriesUnfixed": boolean
    }
  ],
  "races": [
    {
      "id": "string",
      "number": number,
      "name": "string",
      "startAt": number,
      "cupId": "string",
      "scheduleId": "string",
      "distance": number,
      "lap": number,
      "entriesNumber": number,
      "class": "string",
      "raceType": "string",
      "raceType3": "string",
      "isGradeRace": boolean,
      "status": "string",
      "weather": "string",
      "windSpeed": number,
      "cancel": boolean,
      "cancelReason": "string",
      "closeAt": number,
      "decidedAt": number,
      "hasDigestVideo": boolean,
      "digestVideo": "string",
      "digestVideoProvider": "string"
    }
  ]
}
```

### レスポンスフィールド
| フィールド | 型 | 説明 |
|------------|----|----|
| `cup` | object | 開催の基本情報 |
| `cup.id` | string | 開催のユニークID |
| `cup.name` | string | 開催名（例: "高松宮記念杯"） |
| `cup.startDate` | string | 開催の開始日（YYYY-MM-DD形式） |
| `cup.endDate` | string | 開催の終了日（YYYY-MM-DD形式） |
| `cup.duration` | number | 開催日数 |
| `cup.grade` | number | 開催のグレード（1: F2, 2: F1, 3: G3, 4: G2, 5: G1） |
| `cup.venueId` | string | 競輪場ID |
| `cup.labels` | array | 開催に関するラベル情報の配列 |
| `cup.playersUnfixed` | boolean | 選手未確定フラグ |
| `schedules` | array | 開催日程の配列 |
| `schedules[].id` | string | スケジュールID |
| `schedules[].date` | string | 開催日（YYYY-MM-DD形式） |
| `schedules[].day` | number | 開催日数の中の何日目か（例: 1, 2, 3...） |
| `schedules[].cupId` | string | 開催ID |
| `schedules[].index` | number | 中止・延期を問わず開催日数の中の何日目か（後続のOddsAPIとRaceAPIリクエストで使用） |
| `schedules[].entriesUnfixed` | boolean | 出走表未確定フラグ |
| `races` | array | レース情報の配列 |
| `races[].id` | string | レースID |
| `races[].number` | number | レース番号（1R, 2R, ...） |
| `races[].name` | string | レース名 |
| `races[].startAt` | number | レース開始時刻（UNIXタイムスタンプ） |
| `races[].cupId` | string | 開催ID |
| `races[].scheduleId` | string | スケジュールID |
| `races[].distance` | number | レース距離（メートル） |
| `races[].lap` | number | 周回数 |
| `races[].entriesNumber` | number | 出走選手数 |
| `races[].class` | string | レースクラス |
| `races[].raceType` | string | レース種別 |
| `races[].raceType3` | string | レース種別（3区分） |
| `races[].isGradeRace` | boolean | グレードレースフラグ |
| `races[].status` | string | レースステータス |
| `races[].weather` | string | 天候 |
| `races[].windSpeed` | number | 風速 |
| `races[].cancel` | boolean | レースキャンセルフラグ |
| `races[].cancelReason` | string | キャンセル理由 |
| `races[].closeAt` | number | 締切時刻 |
| `races[].decidedAt` | number | 確定時刻 |
| `races[].hasDigestVideo` | boolean | ダイジェスト動画有無 |
| `races[].digestVideo` | string | ダイジェスト動画情報 |
| `races[].digestVideoProvider` | string | 動画プロバイダー |

## サンプルリクエスト

### cURL
```bash
curl -X GET "https://api.winticket.jp/v1/keirin/cups/2025031813?fields=cup,schedules,races&pfm=web"
```

### Python
```python
import requests
from datetime import datetime

cup_id = "2025031813"
url = f"https://api.winticket.jp/v1/keirin/cups/{cup_id}"
params = {
    "fields": "cup,schedules,races",
    "pfm": "web"
}

response = requests.get(url, params=params)
data = response.json()

# 基本情報の表示
cup = data.get("cup", {})
print(f"{cup.get('name')} ({cup.get('grade')})")
print(f"{cup.get('startDate')} 〜 {cup.get('endDate')} ({cup.get('duration')}日間)")

# スケジュール情報の表示
schedules = data.get("schedules", [])
for schedule in schedules:
    print(f"第{schedule.get('day')}日: {schedule.get('date')} (index={schedule.get('index')})")

# レース情報の表示
races = data.get("races", [])
print(f"合計レース数: {len(races)}")
for race in races[:3]:  # 最初の3レースのみ表示
    start_time = datetime.fromtimestamp(race.get("startAt", 0)).strftime("%H:%M")
    print(f"{race.get('number')}R: {race.get('name')} ({start_time}) {race.get('distance')}m")
```

## サンプルレスポンス
```json
{
  "cup": {
    "id": "2025031813",
    "name": "FⅠ第7回福井OCバンク開設記念",
    "startDate": "2025-03-18",
    "endDate": "2025-03-20",
    "duration": 3,
    "grade": 2,
    "venueId": "FUKUI",
    "labels": ["F1"],
    "playersUnfixed": false
  },
  "schedules": [
    {
      "id": "2025031813_1",
      "date": "2025-03-18",
      "day": 1,
      "cupId": "2025031813",
      "index": 1,
      "entriesUnfixed": false
    },
    {
      "id": "2025031813_2",
      "date": "2025-03-19",
      "day": 2,
      "cupId": "2025031813",
      "index": 2,
      "entriesUnfixed": false
    },
    {
      "id": "2025031813_3",
      "date": "2025-03-20",
      "day": 3,
      "cupId": "2025031813",
      "index": 3,
      "entriesUnfixed": false
    }
  ],
  "races": [
    {
      "id": "2025031813_1_1",
      "number": 1,
      "name": "初日特選",
      "startAt": 1774183200,
      "cupId": "2025031813",
      "scheduleId": "2025031813_1",
      "distance": 2000,
      "lap": 5,
      "entriesNumber": 9,
      "class": "A級",
      "raceType": "予選",
      "raceType3": "予選",
      "status": "確定",
      "weather": "晴れ",
      "windSpeed": 2.0
    },
    {
      "id": "2025031813_1_2",
      "number": 2,
      "name": "F1一般戦",
      "startAt": 1774185900,
      "cupId": "2025031813",
      "scheduleId": "2025031813_1",
      "distance": 2000,
      "lap": 5,
      "entriesNumber": 7,
      "class": "A級",
      "raceType": "一般",
      "raceType3": "一般",
      "status": "確定",
      "weather": "晴れ",
      "windSpeed": 2.0
    }
  ]
}
```

## エラーレスポンス

### 404 Not Found
```json
{
  "error": {
    "code": "CupNotFound",
    "message": "指定された開催IDが見つかりません"
  }
}
```

## 関連エンドポイント
- [月間開催一覧 API](./WinticketCupsAPI.md) - 開催IDを取得するために使用
- [レース詳細 API](./WinticketRaceAPI.md) - レースの詳細情報を取得するために使用
- [venue情報 API](./WinticketVenueAPI.md) - 競輪場の詳細情報を取得するために使用

## 注意事項
- 開催IDとスケジュールのindexは、レースの詳細情報を取得する際に必要となります
- レースの開始時刻（startAt）はUNIXタイムスタンプ形式で提供されます
- 競輪場の詳細情報や選手情報は、それぞれ専用のAPIを使用して取得してください
- `fields`パラメータでレスポンスに含めるセクションを指定できます
- 同じレース番号でも日程が異なる場合があります（例: 1日目の7R、2日目の7R） 