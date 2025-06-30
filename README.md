# 競輪データ更新ツール

競輪のレース情報、オッズ、結果、周回データなどを取得して保存するツールです。GUIモードとコマンドラインモードの両方をサポートしています。

## 機能

- [Winticket](https://www.winticket.jp/)からレース情報、出走表、オッズ、結果を取得
- [競輪公式サイト](https://keirin.jp/)から周回データを取得
- 日付指定での更新
- 期間指定での一括更新
- 自動更新機能
- SQLiteデータベースに保存
- **NEW**: コマンドラインモードでの自動化対応

## インストール

```bash
# リポジトリをクローン
git clone https://github.com/yourusername/keirin-data-updater.git
cd keirin-data-updater

# 依存パッケージのインストール
pip install -r requirements.txt
```

## 使い方

### GUIモード（従来通り）

```bash
# GUIモードで起動
python main.py
```

### コマンドラインモード（NEW）

#### 基本構文
```bash
python main.py --mode [モード] [オプション]
```

#### よく使用されるコマンド

```bash
# 通常の更新（前後2日分、全ステップ）
python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1

# 期間指定更新
python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 1 --step3 1 --step4 1 --step5 1

# セットアップ（全データ）
python main.py --mode setup --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --force-update 1

# ドライラン（確認のみ）
python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --dry-run 1
```

#### クイック実行スクリプト

**Windows:**
```bash
scripts\quick_update.bat
```

**Linux/macOS:**
```bash
./scripts/quick_update.sh
```

#### コマンドライン引数

| 引数 | 説明 | 値 |
|------|------|-----|
| `--mode` | 更新モード | `check_update`, `period`, `setup` |
| `--start-date` | 開始日（期間指定モード用） | `YYYY-MM-DD` |
| `--end-date` | 終了日（期間指定モード用） | `YYYY-MM-DD` |
| `--step1` ～ `--step5` | 各ステップの実行指定 | `0` (実行しない), `1` (実行する) |
| `--force-update` | 強制更新モード | `0` (通常), `1` (強制更新) |
| `--venue-codes` | 対象会場コード | 例: `01 02 03` |
| `--debug` | デバッグモード | `0` (通常), `1` (詳細ログ) |
| `--max-workers` | 並列処理数 | 数値 |
| `--dry-run` | ドライランモード | `0` (実行), `1` (確認のみ) |

詳細な使用方法は [docs/CLI_USAGE.md](docs/CLI_USAGE.md) を参照してください。

### 更新対象の設定

1. 日付指定: 単一の日付を指定して更新
2. 期間指定: 開始日から終了日までの範囲を更新
3. 全期間更新: 過去30日分のデータを一括更新

### データソースの選択

- Winticketデータ: レース情報、出走表、オッズ、結果などの基本情報
- Yenjoyデータ: 周回データなどの詳細情報

## プロジェクト構成

```
keirin-data-updater/
├── api/                  # APIクライアント
│   ├── winticket_api.py  # Winticket APIクライアント
│   └── yenjoy_api.py     # Yenjoy APIクライアント
├── database/             # データベース関連
│   ├── db_accessor.py    # データベースアクセサ
│   ├── db_initializer.py # データベース初期化
│   └── keirin_database.py # データベース操作
├── gui/                  # GUI関連
│   ├── keirin_updater_gui.py # メインGUIクラス
│   ├── log_manager.py    # ログ管理
│   ├── ui_builder.py     # UI構築
│   └── update_manager.py # 更新処理管理
├── logs/                 # ログファイル格納ディレクトリ
├── main.py               # エントリポイント
├── requirements.txt      # 依存パッケージリスト
└── README.md             # このファイル
```

## ライセンス

MIT

## 免責事項

このツールは個人的な利用を目的としています。データの取得・利用にあたっては、各サイトの利用規約を遵守してください。取得したデータの利用によって生じたいかなる損害についても、開発者は責任を負いません。

## データベース利用の注意点

### データ保存時の注意点

1. **トランザクションの明示的なコミット**
   - データ保存後は必ず `commit_transaction()` を呼び出すか、`with db.transaction()` ブロックを使用してください
   - `upsert_dataframe()` は外部トランザクションがない場合のみ自動コミットします

2. **シャードデータベースの仕組み**
   - スレッドごとに別のシャードファイル（`database/shards/keirindata_shard_X.db`）が使用されます
   - シャード間のデータは自動的に統合されません
   - 全データの検索には `KeirinDatabase.query_all_shards()` を使用してください

3. **DuckDBエクスポート**
   - 分析用にデータを統合する場合は `KeirinDatabase.export_to_duckdb()` を使用してください
   - エクスポートは手動で実行する必要があります
   - エクスポート前にすべてのデータが正しく保存されていることを確認してください

### データベース処理の基本パターン

```python
# 基本的なデータ保存パターン
def save_data(data_df):
    try:
        # データベースに接続
        db = KeirinDatabase()
        
        # トランザクション開始
        with db.transaction():  # このブロックで自動的にコミットされます
            db.upsert_dataframe(data_df, 'テーブル名', ['キーカラム'])
        
        return True
    except Exception as e:
        logger.error(f"データ保存エラー: {e}")
        return False

# または明示的なトランザクション管理
def save_data_explicit(data_df):
    db = KeirinDatabase()
    try:
        # トランザクション開始
        db.begin_transaction()
        
        # データ保存
        db.upsert_dataframe(data_df, 'テーブル名', ['キーカラム'])
        
        # 明示的にコミット
        db.commit_transaction()
        
        return True
    except Exception as e:
        # エラー時はロールバック
        db.rollback_transaction()
        logger.error(f"データ保存エラー: {e}")
        return False 