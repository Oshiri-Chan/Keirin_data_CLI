# 競輪データ更新ツール - コマンドラインモード使用方法

## 概要

競輪データ更新ツールは、GUIモードに加えてコマンドラインモードも提供しています。コマンドラインモードでは、スクリプトやバッチ処理での自動化、詳細な設定制御が可能です。

## 基本構文

```bash
python main.py [オプション]
```

## 引数なしの実行

引数を指定しない場合、GUIモードで起動します：

```bash
python main.py
```

## コマンドライン引数

### 必須引数

#### `--mode`
更新モードを指定します。以下の3つから選択：

- `check_update`: 更新日から前後2日分をチェック・更新
- `period`: 期間指定更新（`--start-date` と `--end-date` が必要）
- `setup`: 2018年から現在までの全データ保存

### 更新期間の指定

#### `--start-date` / `--end-date`
期間指定モード（`--mode period`）でのみ使用します。

- 形式: `YYYY-MM-DD`
- 例: `--start-date 2024-01-01 --end-date 2024-01-31`

### ステップの指定

各ステップの実行を個別に制御できます：

#### `--step1` / `--step2` / `--step3` / `--step4` / `--step5`
- `0`: 実行しない
- `1`: 実行する
- デフォルト: `0`

### その他のオプション

#### `--force-update`
強制更新モードの設定：
- `0`: 通常更新（デフォルト）
- `1`: 強制更新

#### `--venue-codes`
処理対象の会場コードを指定（複数指定可能）：
```bash
--venue-codes 01 02 03
```

#### `--debug`
デバッグモードの設定：
- `0`: 通常ログ（デフォルト）
- `1`: 詳細ログ

#### `--max-workers`
最大並列処理数を指定：
```bash
--max-workers 10
```

#### `--dry-run`
ドライランモードの設定：
- `0`: 実際に更新を実行（デフォルト）
- `1`: 処理内容のみ表示、実際の更新は行わない

## 使用例

### 1. 通常の更新（前後2日分、全ステップ）

```bash
python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1
```

### 2. 期間指定更新

```bash
python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 1 --step3 1 --step4 1 --step5 1
```

### 3. セットアップ（全データ）

```bash
python main.py --mode setup --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --force-update 1
```

### 4. 特定のステップのみ実行

```bash
# ステップ1とステップ3のみ実行
python main.py --mode check_update --step1 1 --step3 1
```

### 5. 特定の会場のみ更新

```bash
python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 1 --venue-codes 01 02 03
```

### 6. ドライラン（確認のみ）

```bash
python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --dry-run 1
```

### 7. デバッグモード

```bash
python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --debug 1
```

### 8. 並列処理数指定

```bash
python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --max-workers 10
```

### 9. 強制更新

```bash
python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --force-update 1
```

## 便利なスクリプト

### Windows用バッチファイル

#### クイック実行
```bash
scripts\quick_update.bat
```

#### 使用例表示
```bash
scripts\cli_examples.bat
```

### Linux/macOS用シェルスクリプト

#### クイック実行
```bash
./scripts/quick_update.sh
```

## 各ステップの説明

- **ステップ1**: レース開催情報の取得
- **ステップ2**: 出走選手情報の取得
- **ステップ3**: レース詳細情報の取得
- **ステップ4**: オッズ情報の取得
- **ステップ5**: 結果情報の取得

## 更新モードの詳細

### check_update
- 現在日から前後2日分のデータをチェック・更新
- 日常的な更新に適している
- 処理時間は比較的短い

### period
- 指定した期間のデータを更新
- 過去の特定期間のデータを再取得したい場合に使用
- 期間によっては処理時間が長くなる

### setup
- 2018年から現在までの全データを保存
- 初回セットアップや完全なデータ再構築に使用
- 非常に長時間の処理になる（数時間～数日）

## 注意事項

1. **セットアップモード**は大量のデータを処理するため、完了まで長時間かかります
2. **強制更新モード**では既存データも再取得するため、処理時間が増加します
3. **デバッグモード**ではログ出力が増加するため、ログファイルのサイズにご注意ください
4. **並列処理数**を増やしすぎると、APIのレート制限に引っかかる可能性があります

## トラブルシューティング

### エラーが発生した場合
1. `--debug 1` を追加してデバッグモードで実行
2. ログファイルの内容を確認
3. `--dry-run 1` で処理内容を事前確認

### 処理が遅い場合
1. `--max-workers` で並列処理数を調整
2. 特定のステップのみ実行して問題を特定
3. 会場コードを指定して処理範囲を限定

### データに問題がある場合
1. `--force-update 1` で強制更新を実行
2. 特定期間のみを再処理
3. セットアップモードで完全再構築

## ヘルプの表示

コマンドライン引数の詳細なヘルプを表示：

```bash
python main.py --help
``` 