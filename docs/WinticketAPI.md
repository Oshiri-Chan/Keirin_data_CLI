# Winticket API

## 概要
競輪（KEIRIN）の公式オンラインベッティングサービス「Winticket」が提供するAPIについての概要ドキュメントです。

## ベースURL
```
https://api.winticket.jp/v1/keirin
```

## 基本情報
- **API形式**: RESTful API
- **レスポンス形式**: JSON
- **機能**: 開催情報、レース結果、オッズ情報などの取得
- **認証**: 不要

## 共通パラメータ
| パラメータ | 型 | 必須 | 説明 |
|------------|----|----|------|
| `pfm` | string | いいえ | プラットフォーム（デフォルト: "web"） |
| `fields` | string | いいえ | 取得するフィールドを制限（カンマ区切り） |

## 主要エンドポイント

| エンドポイント | 説明 | 詳細ドキュメント |
|----------------|------|-----------------|
| `/cups` | 月間開催一覧の取得 | [WinticketCupsAPI.md](./WinticketCupsAPI.md) |
| `/cups/{cup_id}` | 開催詳細情報の取得 | [WinticketCupDetailAPI.md](./WinticketCupDetailAPI.md) |
| `/cups/{cup_id}/schedules/{schedule_id}/races/{race_number}` | レース詳細情報の取得 | [WinticketRaceAPI.md](./WinticketRaceAPI.md) |
| `/cups/{cup_id}/schedules/{schedule_id}/races/{race_number}/odds` | オッズ情報の取得 | [WinticketOddsAPI.md](./WinticketOddsAPI.md) |

## エラー処理
一般的なHTTPステータスコードが使用されます：

| ステータスコード | 説明 |
|-----------------|------|
| 200 OK | リクエスト成功 |
| 400 Bad Request | パラメータ不足などの不正なリクエスト |
| 404 Not Found | リソースが見つからない |
| 500 Internal Server Error | サーバー側のエラー |

## 実装例

```python
from src.winticket_api import WinticketAPI

# APIクライアントのインスタンス化
api = WinticketAPI()

# 月間開催情報の取得（2025年3月）
monthly_data = api.get_monthly_cups("20250301")

# 開催詳細情報の取得
cup_id = "20250301MUKW"
cup_detail = api.get_cup_detail(cup_id)

# レース結果の取得
race_results = api.get_race_results_dataframe(cup_id, "scheduleId", 1)
```

## 注意事項
- APIへの過度なリクエストは避けてください
- データの取得間隔を適切に設定することを推奨します
- 取得したデータの利用は個人的な目的に限定してください
- APIの仕様は予告なく変更される可能性があります 