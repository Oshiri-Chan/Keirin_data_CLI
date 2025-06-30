# Winticket Race API

## 概要
特定のレースの詳細情報（出走表、結果、選手情報など）を取得するためのAPIエンドポイントです。

## エンドポイント
```
GET /cups/{cup_id}/schedules/{index}/races/{race_number}
```

## パラメータ
### パスパラメータ
| パラメータ | 型 | 必須 | 説明 |
|------------|----|----|------|
| `cup_id` | string | はい | 開催ID（例: `2025031813`） |
| `index` | number | はい | 開催のインデックス番号（例: `1`, `2`） |
| `race_number` | number | はい | レース番号（例: `1`, `2`） |

### クエリパラメータ
| パラメータ | 型 | 必須 | 説明 |
|------------|----|----|------|
| `fields` | string | いいえ | 取得するフィールド（カンマ区切り、例: "race,entries,players,records"） |
| `pfm` | string | いいえ | プラットフォーム（デフォルト: "web"） |

## レスポンス

### 成功時 (200 OK)
```json
{
  "race": {
    "id": "string",
    "number": number,
    "name": "string",
    "startAt": number,
    "cupId": "string",
    "scheduleId": "string",
    "distance": number,
    "lap": number,
    "bikeCount": number,
    "grade": number,
    "condition": string,
    "temperature": number,
    "humidity": number,
    "weather": string,
    "windSpeed": number,
    "status": string,
    "cancel": boolean,
    "cancelReason": string,
    "class": string,
    "closeAt": number,
    "decidedAt": number,
    "entriesNumber": number,
    "raceType": string,
    "raceType3": string,
    "isGradeRace": boolean,
    "hasDigestVideo": boolean,
    "digestVideo": string,
    "digestVideoProvider": string,
    "winner": {
      "entryId": "string",
      "number": number
    }
  },
  "entries": [
    {
      "id": "string",
      "number": number,
      "raceId": "string",
      "playerId": "string",
      "class": number,
      "style": number,
      "stylePoint": number,
      "absent": boolean,
      "bracketNumber": number,
      "hasPreviousClassGroup": boolean,
      "playerCurrentTermClass": string,
      "playerCurrentTermGroup": string,
      "playerPreviousTermClass": string,
      "playerPreviousTermGroup": string
    }
  ],
  "players": [
    {
      "id": "string",
      "name": "string",
      "nameShort": "string",
      "nameKana": "string",
      "name5": "string",
      "yomi": "string",
      "age": number,
      "birthday": "YYYY-MM-DD",
      "prefecture": "string",
      "rank": number,
      "gear": number,
      "class": string,
      "gender": string,
      "group": string,
      "portraitImage": "string",
      "regionId": string,
      "term": number
    }
  ],
  "records": [
    {
      "playerId": "string",
      "standing": number,
      "time": number,
      "gearRatio": number,
      "style": string,
      "racePoint": number,
      "racePointStr": string,
      "comment": "string",
      "first": number,
      "second": number,
      "third": number,
      "others": number,
      "firstRate": number,
      "secondRate": number,
      "thirdRate": number,
      "firstRateStr": string,
      "secondRateStr": string,
      "thirdRateStr": string,
      "marker": string,
      "predictionMark": string,
      "back": number,
      "deepCloser": boolean,
      "frontRunner": boolean,
      "stalker": boolean,
      "gearRatioStr": string,
      "hasModifiedGearRatio": boolean,
      "modifiedGearRatio": number,
      "modifiedGearRatioStr": string,
      "currentCupResults": [],
      "previousCupResults": [],
      "latestCupResults": [],
      "latestVenueResults": [],
      "previousCupId": "string",
      "exCompete": {
        "succeeded": number,
        "total": number,
        "percentage": number
      },
      "exLeftBehind": {
        "succeeded": number,
        "total": number,
        "percentage": number
      },
      "exSnatch": {
        "succeeded": number,
        "total": number,
        "percentage": number
      },
      "exSplitLine": {
        "succeeded": number,
        "total": number,
        "percentage": number
      },
      "exSpurt": {
        "succeeded": number,
        "total": number,
        "percentage": number
      },
      "exThrust": {
        "succeeded": number,
        "total": number,
        "percentage": number
      },
      "lineCompete": {
        "first": number,
        "second": number,
        "third": number,
        "others": number,
        "total": number,
        "firstPercentage": number,
        "secondPercentage": number,
        "thirdPercentage": number,
        "othersPercentage": number
      },
      "linePositionFirst": {
        "first": number,
        "second": number,
        "third": number,
        "others": number,
        "total": number,
        "firstPercentage": number,
        "secondPercentage": number,
        "thirdPercentage": number,
        "othersPercentage": number
      },
      "linePositionSecond": {
        "first": number,
        "second": number,
        "third": number,
        "others": number,
        "total": number,
        "firstPercentage": number,
        "secondPercentage": number,
        "thirdPercentage": number,
        "othersPercentage": number
      },
      "linePositionThird": {
        "first": number,
        "second": number,
        "third": number,
        "others": number,
        "total": number,
        "firstPercentage": number,
        "secondPercentage": number,
        "thirdPercentage": number,
        "othersPercentage": number
      },
      "lineSingleHorseman": {
        "first": number,
        "second": number,
        "third": number,
        "others": number,
        "total": number,
        "firstPercentage": number,
        "secondPercentage": number,
        "thirdPercentage": number,
        "othersPercentage": number
      }
    }
  ],
  "linePrediction": {
    "lineType": string,
    "lines": [
      {
        "entries": [
          {
            "numbers": [number]
          }
        ]
      }
    ]
  },
  "digests": [
    {
      "type": number,
      "title": "string",
      "content": "string"
    }
  ]
}
```

### レスポンスフィールド
| フィールド | 型 | 説明 |
|------------|----|----|
| `race` | object | レースの基本情報 |
| `race.id` | string | レースのユニークID |
| `race.number` | number | レース番号 |
| `race.name` | string | レース名（例: "予選", "決勝"） |
| `race.startAt` | number | レース開始時刻（UNIXタイムスタンプ） |
| `race.distance` | number | レース距離（メートル） |
| `race.lap` | number | 周回数 |
| `race.condition` | string | コンディション（天候など） |
| `race.temperature` | number | 気温（℃） |
| `race.humidity` | number | 湿度（%） |
| `race.weather` | string | 天候 |
| `race.windSpeed` | number | 風速 |
| `race.status` | string | レースステータス |
| `race.cancel` | boolean | レースキャンセルフラグ |
| `race.cancelReason` | string | キャンセル理由 |
| `race.class` | string | レースクラス |
| `race.closeAt` | number | 締切時刻 |
| `race.decidedAt` | number | 確定時刻 |
| `race.entriesNumber` | number | エントリー数 |
| `race.raceType` | string | レース種別 |
| `race.raceType3` | string | レース種別（3区分） |
| `race.isGradeRace` | boolean | グレードレースフラグ |
| `race.hasDigestVideo` | boolean | ダイジェスト動画有無 |
| `race.digestVideo` | string | ダイジェスト動画情報 |
| `race.digestVideoProvider` | string | 動画プロバイダー |
| `race.winner` | object | 勝者情報 |
| `entries` | array | レースのエントリー（出走表）情報 |
| `entries[].id` | string | エントリーID |
| `entries[].number` | number | 車番 |
| `entries[].playerId` | string | 選手ID |
| `entries[].class` | number | 選手クラス（1: S級S班, 2: S級1班, 3: S級2班, 4: A級1班, 5: A級2班） |
| `entries[].style` | number | 選手の脚質番号（1: 逃げ, 2: 捲り, 3: 差し, 4: マーク, 5: 追込） |
| `entries[].stylePoint` | number | 脚質指数 |
| `entries[].absent` | boolean | 欠場フラグ（true: 欠場, false: 出走） |
| `entries[].bracketNumber` | number | 枠番号 |
| `entries[].hasPreviousClassGroup` | boolean | 前期級班有無 |
| `entries[].playerCurrentTermClass` | string | 当期選手級班 |
| `entries[].playerCurrentTermGroup` | string | 当期選手グループ |
| `entries[].playerPreviousTermClass` | string | 前期選手級班 |
| `entries[].playerPreviousTermGroup` | string | 前期選手グループ |
| `players` | array | 選手情報の配列 |
| `players[].id` | string | 選手ID |
| `players[].name` | string | 選手名 |
| `players[].nameShort` | string | 選手名（短縮版） |
| `players[].nameKana` | string | 選手名カナ |
| `players[].name5` | string | 選手名（5文字） |
| `players[].yomi` | string | 選手名読み |
| `players[].age` | number | 年齢 |
| `players[].birthday` | string | 生年月日 |
| `players[].prefecture` | string | 所属（都道府県） |
| `players[].rank` | number | 選手ランク |
| `players[].gear` | number | 標準ギア |
| `players[].class` | string | 選手級班 |
| `players[].gender` | string | 性別 |
| `players[].group` | string | 選手グループ |
| `players[].portraitImage` | string | 選手画像 |
| `players[].regionId` | string | 地域ID |
| `players[].term` | number | 期 |
| `records` | array | レース結果の記録 |
| `records[].playerId` | string | 選手ID |
| `records[].standing` | number | 着順（1: 1着, 2: 2着, ...） |
| `records[].time` | number | 走破タイム（秒） |
| `records[].gearRatio` | number | 使用ギア比 |
| `records[].gearRatioStr` | string | 使用ギア比（文字列） |
| `records[].style` | string | 使用脚質（例: "逃げ", "差し"） |
| `records[].racePoint` | number | レースポイント |
| `records[].racePointStr` | string | レースポイント（文字列） |
| `records[].comment` | string | レースコメント |
| `records[].first` | number | 1着回数 |
| `records[].second` | number | 2着回数 |
| `records[].third` | number | 3着回数 |
| `records[].others` | number | その他着順回数 |
| `records[].firstRate` | number | 1着率 |
| `records[].secondRate` | number | 2着率 |
| `records[].thirdRate` | number | 3着率 |
| `records[].firstRateStr` | string | 1着率（文字列） |
| `records[].secondRateStr` | string | 2着率（文字列） |
| `records[].thirdRateStr` | string | 3着率（文字列） |
| `records[].marker` | string | マーカー |
| `records[].predictionMark` | string | 予想マーク |
| `records[].back` | number | 差し |
| `records[].deepCloser` | boolean | 追込フラグ |
| `records[].frontRunner` | boolean | 逃げフラグ |
| `records[].stalker` | boolean | マークフラグ |
| `records[].hasModifiedGearRatio` | boolean | ギア比修正有無 |
| `records[].modifiedGearRatio` | number | 修正ギア比 |
| `records[].modifiedGearRatioStr` | string | 修正ギア比（文字列） |
| `records[].currentCupResults` | array | 当期開催レース結果 |
| `records[].previousCupResults` | array | 前回開催レース結果 |
| `records[].latestCupResults` | array | 最新開催レース結果 |
| `records[].latestVenueResults` | array | 最新会場レース結果 |
| `records[].previousCupId` | string | 前回開催ID |
| `records[].exCompete` | object | 競り合い統計 |
| `records[].exLeftBehind` | object | 左差し統計 |
| `records[].exSnatch` | object | 差し統計 |
| `records[].exSplitLine` | object | ライン分割統計 |
| `records[].exSpurt` | object | スパート統計 |
| `records[].exThrust` | object | 突き抜け統計 |
| `records[].lineCompete` | object | ライン競り合い統計 |
| `records[].linePositionFirst` | object | 1番手ライン統計 |
| `records[].linePositionSecond` | object | 2番手ライン統計 |
| `records[].linePositionThird` | object | 3番手ライン統計 |
| `records[].lineSingleHorseman` | object | 単騎ライン統計 |
| `linePrediction` | object | ラインの予想情報 |
| `linePrediction.lineType` | string | ライン種別 |
| `linePrediction.lines` | array | ライン情報 |
| `linePrediction.lines[].entries` | array | ライン別エントリー |
| `linePrediction.lines[].entries[].numbers` | array | 車番リスト |
| `digests` | array | レースの要約情報 |

## サンプルリクエスト

### cURL
```bash
curl -X GET "https://api.winticket.jp/v1/keirin/cups/2025031813/schedules/1/races/1?fields=race,entries,players&pfm=web"
```

### Python
```python
import requests
from datetime import datetime

cup_id = "2025031813"
index = 1
race_number = 1

url = f"https://api.winticket.jp/v1/keirin/cups/{cup_id}/schedules/{index}/races/{race_number}"
params = {
    "fields": "race,entries,players,records",
    "pfm": "web"
}

response = requests.get(url, params=params)
data = response.json()

# レース情報の表示
race = data.get("race", {})
race_time = datetime.fromtimestamp(race.get("startAt", 0)).strftime("%Y-%m-%d %H:%M")
print(f"レース: {race.get('name')} ({race_time})")
print(f"距離: {race.get('distance')}m ({race.get('lap')}周)")

# 出走表の表示
entries = data.get("entries", [])
players = {p["id"]: p for p in data.get("players", [])}
print("\n出走表:")
for entry in sorted(entries, key=lambda x: x["number"]):
    player = players.get(entry.get("playerId"), {})
    print(f"  {entry['number']}番: {player.get('name')} ({player.get('prefecture')})")

# 結果の表示
records = data.get("records", [])
if records:
    print("\n結果:")
    for record in sorted(records, key=lambda x: x["standing"]):
        player_id = record.get("playerId")
        player_name = players.get(player_id, {}).get("name", "不明")
        print(f"  {record['standing']}着: {player_name} ({record.get('time')}秒)")
```

## サンプルレスポンス
```json
{
  "race": {
    "id": "2025031813_1_1",
    "number": 1,
    "name": "初日特選",
    "startAt": 1774183200,
    "cupId": "2025031813",
    "scheduleId": "2025031813_1",
    "distance": 2000,
    "lap": 5,
    "bikeCount": 9,
    "grade": 0,
    "condition": "晴れ",
    "temperature": 18.5,
    "humidity": 65,
    "weather": "晴れ",
    "windSpeed": 2.5,
    "status": "終了",
    "winner": {
      "entryId": "2025031813_1_1_01",
      "number": 1
    }
  },
  "entries": [
    {
      "id": "2025031813_1_1_01",
      "number": 1,
      "raceId": "2025031813_1_1",
      "playerId": "123456",
      "class": 1,
      "style": 1,
      "stylePoint": 85,
      "absent": false,
      "bracketNumber": 1
    },
    {
      "id": "2025031813_1_1_02",
      "number": 2,
      "raceId": "2025031813_1_1",
      "playerId": "234567",
      "class": 2,
      "style": 3,
      "stylePoint": 75,
      "absent": false,
      "bracketNumber": 2
    }
  ],
  "players": [
    {
      "id": "123456",
      "name": "競輪 太郎",
      "nameShort": "競輪太",
      "nameKana": "ケイリン タロウ",
      "age": 28,
      "birthday": "1996-05-10",
      "prefecture": "福井県",
      "rank": 1,
      "gear": 4.35
    },
    {
      "id": "234567",
      "name": "競輪 次郎",
      "nameShort": "競輪次",
      "nameKana": "ケイリン ジロウ",
      "age": 32,
      "birthday": "1992-11-20",
      "prefecture": "石川県",
      "rank": 3,
      "gear": 4.40
    }
  ],
  "records": [
    {
      "playerId": "123456",
      "standing": 1,
      "time": 121.5,
      "gearRatio": 4.35,
      "style": "逃げ",
      "racePoint": 10,
      "comment": "好スタートで逃げ切り"
    },
    {
      "playerId": "234567",
      "standing": 2,
      "time": 121.8,
      "gearRatio": 4.40,
      "style": "差し",
      "racePoint": 8,
      "comment": "良い脚で迫るも及ばず"
    }
  ]
}
```

## 関連エンドポイント
- [開催詳細 API](./WinticketCupDetailAPI.md) - 開催とインデックス番号を取得
- [オッズ API](./WinticketOddsAPI.md) - オッズ情報を取得

## 注意事項
- `fields` パラメータを使用して必要なデータのみを取得することで、レスポンスのサイズを削減できます
- レース結果は、レース終了後にのみ提供されます
- 選手情報と出走表情報を組み合わせることで、詳細な分析が可能です 