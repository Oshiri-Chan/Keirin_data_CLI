# 競輪データ更新ツール

競輪のレース情報、オッズ、結果、周回データなどを取得して保存するコマンドラインツールです。

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

### コマンドラインの基本

- 利用可能なコマンド: `update` / `status` / `config` / `export` / `deploy`

#### 基本構文
```bash
python main.py <command> [options]
```

#### よく使う例（update）

```bash
# 通常の更新（前後2日分）、ステップ1のみ実行
python main.py update --mode check-range --step 1

# 期間指定更新（2024-01-01〜2024-01-31）、ステップ3のみ
python main.py update --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step 3

# 単日更新（当日。明示指定する場合は --date で指定）
python main.py update --mode single-day --step 2
python main.py update --mode single-day --date 2024-03-01 --step 2

# セットアップ（2018年〜現在）。強制更新を無効化する場合
python main.py update --mode setup --step 5 --no-force-update

# 会場指定・並列数・ドライラン・デバッグを併用
python main.py update --mode period --start-date 2024-01-01 --end-date 2024-01-07 \
  --step 1 --venue-codes 01 02 03 --max-workers 10 --dry-run --debug

# 全ステップを順に実行（Windows PowerShell）
for ($i=1; $i -le 5; $i++) { python .\main.py update --mode check-range --step $i }
```

#### update コマンドの主なオプション

- `--mode`: `check-range` | `period` | `single-day` | `setup`
- `--step`: 実行ステップ番号（必須）1〜5 のいずれか
- `--start-date` / `--end-date`: 期間指定（`--mode period`）
  - 未指定時は日本時間基準で自動設定（前日〜翌日）
- `--date`: 単日指定（`--mode single-day`）
  - 未指定時は日本時間の当日を自動設定
- `--force-update` / `--no-force-update`: 強制更新の有効/無効（デフォルトは有効）
- `--venue-codes`: 会場コードを複数指定可能（例: `01 02 03`）
- `--max-workers`: 並列処理数
- `--dry-run`: 実行せず処理内容のみ表示
- `--debug`: 詳細ログを有効化

#### その他のコマンド

- `status`: システム状態確認
  - `--database` / `--tables` / `--recent <days>`（デフォルト7）
- `config`: 設定の確認・変更
  - `--show` / `--set <KEY> <VALUE>` / `--test-connection`
- `export`: データエクスポート
  - `--format csv|json|sql` / `--table <NAME>` / `--output <PATH>` / `--start-date` / `--end-date`
- `deploy`: DuckDB へデプロイ
  - `--output <PATH>` / `--tables <NAME...>`

ヘルプの表示:
```bash
python main.py --help
python main.py update --help
```

補足: 付属のバッチ/シェルスクリプトは順次新CLIに対応予定です。直接コマンド実行を推奨します。

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