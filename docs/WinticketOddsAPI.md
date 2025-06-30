# Winticket Odds API

## 概要
特定のレースのオッズ情報を取得するためのAPIエンドポイントです。

## エンドポイント
```
GET /cups/{cup_id}/schedules/{index}/races/{race_number}/odds
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
| `fields` | string | いいえ | 取得するフィールド（空白の場合はすべて取得） |
| `pfm` | string | いいえ | プラットフォーム（デフォルト: "web"） |

## レスポンス

### 成功時 (200 OK)
```json
{
  "bracketQuinella": [
    {
      "brackets": [number, number],
      "odds": number,
      "minOdds": number,
      "maxOdds": number,
      "oddsStr": "string",
      "minOddsStr": "string",
      "maxOddsStr": "string",
      "key": array or string,
      "type": "string",
      "popularityOrder": number,
      "unitPrice": number,
      "payoffUnitPrice": number,
      "absent": boolean,
      "id": "string",
      "raceId": "string"
    }
  ],
  "bracketExacta": [
    {
      "brackets": [number, number],
      "odds": number,
      "minOdds": number,
      "maxOdds": number,
      "oddsStr": "string",
      "minOddsStr": "string",
      "maxOddsStr": "string",
      "key": array or string,
      "type": "string",
      "popularityOrder": number,
      "unitPrice": number,
      "payoffUnitPrice": number,
      "absent": boolean,
      "id": "string",
      "raceId": "string"
    }
  ],
  "quinella": [
    {
      "entryIds": ["string", "string"],
      "numbers": [number, number],
      "odds": number,
      "minOdds": number,
      "maxOdds": number,
      "oddsStr": "string",
      "minOddsStr": "string",
      "maxOddsStr": "string",
      "key": array or string,
      "type": "string",
      "popularityOrder": number,
      "unitPrice": number,
      "payoffUnitPrice": number,
      "absent": boolean,
      "id": "string",
      "raceId": "string"
    }
  ],
  "exacta": [
    {
      "entryIds": ["string", "string"],
      "numbers": [number, number],
      "odds": number,
      "minOdds": number,
      "maxOdds": number,
      "oddsStr": "string",
      "minOddsStr": "string",
      "maxOddsStr": "string",
      "key": array or string,
      "type": "string",
      "popularityOrder": number,
      "unitPrice": number,
      "payoffUnitPrice": number,
      "absent": boolean,
      "id": "string",
      "raceId": "string"
    }
  ],
  "trio": [
    {
      "entryIds": ["string", "string", "string"],
      "numbers": [number, number, number],
      "odds": number,
      "minOdds": number,
      "maxOdds": number,
      "oddsStr": "string",
      "minOddsStr": "string",
      "maxOddsStr": "string",
      "key": array or string,
      "type": "string",
      "popularityOrder": number,
      "unitPrice": number,
      "payoffUnitPrice": number,
      "absent": boolean,
      "id": "string",
      "raceId": "string"
    }
  ],
  "trifecta": [
    {
      "entryIds": ["string", "string", "string"],
      "numbers": [number, number, number],
      "odds": number,
      "minOdds": number,
      "maxOdds": number,
      "oddsStr": "string",
      "minOddsStr": "string",
      "maxOddsStr": "string",
      "key": array or string,
      "type": "string",
      "popularityOrder": number,
      "unitPrice": number,
      "payoffUnitPrice": number,
      "absent": boolean,
      "id": "string",
      "raceId": "string"
    }
  ],
  "quinellaPlace": [
    {
      "entryIds": ["string", "string"],
      "numbers": [number, number],
      "odds": number,
      "minOdds": number,
      "maxOdds": number,
      "oddsStr": "string",
      "minOddsStr": "string",
      "maxOddsStr": "string",
      "key": array or string,
      "type": "string",
      "popularityOrder": number,
      "unitPrice": number,
      "payoffUnitPrice": number,
      "absent": boolean,
      "id": "string",
      "raceId": "string"
    }
  ],
  "favorites": [
    {
      "entryId": "string",
      "favorite": number
    }
  ],
  "updatedAt": number,
  "isFinal": boolean,
  "isDelayed": boolean,
  "payoutStatus": number
}
```

### レスポンスフィールド

#### 共通フィールド
全てのオッズタイプには以下の共通フィールドが含まれます：

| フィールド | 型 | 説明 |
|------------|----|----|
| `id` | string | オッズ情報のユニークID |
| `raceId` | string | レースID |
| `odds` | number | オッズ値 |
| `minOdds` | number | 最小オッズ値（変動範囲下限） |
| `maxOdds` | number | 最大オッズ値（変動範囲上限） |
| `oddsStr` | string | オッズ値の文字列表現 |
| `minOddsStr` | string | 最小オッズ値の文字列表現 |
| `maxOddsStr` | string | 最大オッズ値の文字列表現 |
| `key` | array/string | キー（券種と組み合わせ識別用）、空の配列またはnullの場合あり |
| `type` | string | オッズタイプ識別子 |
| `popularityOrder` | number | 人気順位 |
| `unitPrice` | number | 単位金額（通常100円） |
| `payoffUnitPrice` | number | 払戻金額 |
| `absent` | boolean | 欠場フラグ（一部のオッズタイプのみ） |

#### 各オッズタイプのフィールド

| フィールド | 型 | 説明 |
|------------|----|----|
| `bracketQuinella` | array | 枠連オッズの配列（枠順を問わない2つの枠の組み合わせ） |
| `bracketQuinella[].brackets` | array | 枠番号の配列 |
| `bracketExacta` | array | 枠単オッズの配列（1→2のように着順を予想する枠の組み合わせ） |
| `bracketExacta[].brackets` | array | [1着枠, 2着枠]の配列 |
| `quinella` | array | 二車連オッズの配列（1-2のように順序を問わない） |
| `quinella[].entryIds` | array | エントリーIDの配列 |
| `quinella[].numbers` | array | 車番の配列 |
| `exacta` | array | 二車単オッズの配列（1→2のように着順を予想） |
| `exacta[].entryIds` | array | [1着エントリーID, 2着エントリーID]の配列 |
| `exacta[].numbers` | array | [1着車番, 2着車番]の配列 |
| `quinellaPlace` | array | ワイド（二車複）オッズの配列（2着以内に来る2車の組み合わせ） |
| `quinellaPlace[].entryIds` | array | エントリーIDの配列 |
| `quinellaPlace[].numbers` | array | 車番の配列 |
| `trio` | array | 三連複オッズの配列（着順を問わない3車の組み合わせ） |
| `trio[].entryIds` | array | エントリーIDの配列 |
| `trio[].numbers` | array | 車番の配列 |
| `trifecta` | array | 三連単オッズの配列（1→2→3のように着順を予想） |
| `trifecta[].entryIds` | array | [1着エントリーID, 2着エントリーID, 3着エントリーID]の配列 |
| `trifecta[].numbers` | array | [1着車番, 2着車番, 3着車番]の配列 |
| `favorites` | array | 人気順位情報の配列 |
| `favorites[].entryId` | string | 出走エントリーID |
| `favorites[].favorite` | number | 人気順位（1人気、2人気など） |
| `updatedAt` | number | オッズ情報の更新時刻（UNIXタイムスタンプ） |
| `isFinal` | boolean | 最終オッズかどうか（true: 最終オッズ、false: 暫定オッズ） |
| `isDelayed` | boolean | オッズ発表が遅延しているかどうか |
| `payoutStatus` | number | 払い戻し状態（0: 未払い、1: 払い戻し中、2: 払い戻し完了） |

## サンプルリクエスト

### cURL
```bash
curl -X GET "https://api.winticket.jp/v1/keirin/cups/2025031813/schedules/1/races/1/odds?pfm=web"
```

### Python
```python
import requests
from datetime import datetime

cup_id = "2025031813"
index = 1
race_number = 1

url = f"https://api.winticket.jp/v1/keirin/cups/{cup_id}/schedules/{index}/races/{race_number}/odds"
params = {
    "pfm": "web"
}

response = requests.get(url, params=params)
data = response.json()

# 単勝オッズを表示
if "win" in data and data["win"]:
    print("単勝オッズ:")
    for win in sorted(data["win"], key=lambda x: x["number"]):
        print(f"  {win['number']}号車: {win['odds']}倍 (人気順位: {win['popularityOrder']})")

# 二車連オッズを表示
if "quinella" in data and data["quinella"]:
    print("\n二車連オッズ (上位5件):")
    for q in sorted(data["quinella"], key=lambda x: x["odds"])[:5]:
        print(f"  {q['numbers'][0]}-{q['numbers'][1]}: {q['odds']}倍 (払戻: {q['payoffUnitPrice']}円)")

# オッズの更新時刻を表示
if "updatedAt" in data:
    updated_at = datetime.fromtimestamp(data["updatedAt"]).strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n更新時刻: {updated_at}")
    print(f"最終オッズ: {'はい' if data.get('isFinal', False) else 'いいえ'}")
```

## サンプルレスポンス
```json
{
  "win": [
    {
      "id": "win_2025031813_1_1_01",
      "raceId": "2025031813_1_1",
      "entryId": "2025031813_1_1_01",
      "number": 1,
      "odds": 3.2,
      "minOdds": 3.1,
      "maxOdds": 3.5,
      "oddsStr": "3.2",
      "minOddsStr": "3.1",
      "maxOddsStr": "3.5",
      "key": "1",
      "type": "win",
      "popularityOrder": 2,
      "unitPrice": 100,
      "payoffUnitPrice": 320,
      "absent": false
    },
    {
      "id": "win_2025031813_1_1_02",
      "raceId": "2025031813_1_1",
      "entryId": "2025031813_1_1_02",
      "number": 2,
      "odds": 2.1,
      "minOdds": 2.0,
      "maxOdds": 2.3,
      "oddsStr": "2.1",
      "minOddsStr": "2.0",
      "maxOddsStr": "2.3",
      "key": "2",
      "type": "win",
      "popularityOrder": 1,
      "unitPrice": 100,
      "payoffUnitPrice": 210,
      "absent": false
    }
  ],
  "quinella": [
    {
      "id": "quinella_2025031813_1_1_1_2",
      "raceId": "2025031813_1_1",
      "entryIds": ["2025031813_1_1_01", "2025031813_1_1_02"],
      "numbers": [1, 2],
      "odds": 3.8,
      "minOdds": 3.5,
      "maxOdds": 4.2,
      "oddsStr": "3.8",
      "minOddsStr": "3.5",
      "maxOddsStr": "4.2",
      "key": "1,2",
      "type": "quinella",
      "popularityOrder": 1,
      "unitPrice": 100,
      "payoffUnitPrice": 380
    }
  ],
  "quinellaPlace": [
    {
      "id": "quinellaPlace_2025031813_1_1_1_2",
      "raceId": "2025031813_1_1",
      "entryIds": ["2025031813_1_1_01", "2025031813_1_1_02"],
      "numbers": [1, 2],
      "odds": 1.3,
      "minOdds": 1.2,
      "maxOdds": 1.5,
      "oddsStr": "1.3",
      "minOddsStr": "1.2",
      "maxOddsStr": "1.5",
      "key": "1,2",
      "type": "quinellaPlace",
      "popularityOrder": 1,
      "unitPrice": 100,
      "payoffUnitPrice": 130
    }
  ],
  "updatedAt": 1774182900,
  "isFinal": true,
  "isDelayed": false,
  "payoutStatus": 0
}
```

## 関連エンドポイント
- [開催詳細 API](./WinticketCupDetailAPI.md) - 開催とインデックス番号を取得
- [レース詳細 API](./WinticketRaceAPI.md) - レースの詳細情報を取得

## 注意事項
- オッズは時間の経過とともに変動します
- `isFinal` フラグがtrueの場合は最終オッズです
- payoutStatusを確認することで払い戻しの状態を確認できます
- 枠連・枠単は、bracketQuinellaとbracketExactaとして提供されます
- `minOdds`と`maxOdds`はオッズの変動範囲を示します
- `payoffUnitPrice`は100円単位での払戻金額を表します
- `popularityOrder`は人気順位（1が1番人気）を表します
- `type`フィールドはオッズのタイプを示す識別子です 
- `quinellaPlace`はワイド（2着以内に入る2車の組み合わせ）を示します 