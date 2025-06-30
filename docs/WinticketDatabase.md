# Winticket データベース設計

## 概要
WinticketのAPIから取得したデータを効率的に保存・管理するためのデータベース設計について解説します。

## データベース構造

### テーブル一覧
| テーブル名 | 説明 |
|------------|------|
| `cups` | 開催情報を保存 |
| `venues` | 競輪場情報を保存 |
| `schedules` | 開催日程情報を保存 |
| `races` | レース情報を保存 |
| `entries` | 出走表情報を保存 |
| `players` | 選手情報を保存 |
| `records` | レース結果情報を保存 |
| `odds_win` | 単勝オッズ情報を保存 |
| `odds_place` | ワイドオッズ情報を保存 |
| `odds_bracketquinella` | 枠連オッズ情報を保存 |
| `odds_bracketexacta` | 枠単オッズ情報を保存 |
| `odds_quinella` | 二車連オッズ情報を保存 |
| `odds_exacta` | 二車単オッズ情報を保存 |
| `odds_trio` | 三連複オッズ情報を保存 |
| `odds_trifecta` | 三連単オッズ情報を保存 |

## テーブル定義

### cups テーブル
開催の基本情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | VARCHAR(15) | NO | PK | 開催ID (例: "2025031813") |
| `name` | VARCHAR(100) | NO | | 開催名 |
| `start_date` | DATE | NO | | 開催開始日 |
| `end_date` | DATE | NO | | 開催終了日 |
| `duration` | INT | NO | | 開催日数 |
| `grade` | TINYINT | NO | | 開催グレード(1:F2, 2:F1, 3:G3, 4:G2, 5:G1) |
| `venue_id` | VARCHAR(10) | NO | FK | 競輪場ID |
| `labels` | JSON | YES | | 開催に関するラベル情報の配列 |
| `players_unfixed` | BOOLEAN | NO | | 選手未確定フラグ |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### venues テーブル
競輪場情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | VARCHAR(10) | NO | PK | 競輪場ID (例: "FUKUI") |
| `name` | VARCHAR(50) | NO | | 競輪場名 |
| `prefecture` | VARCHAR(50) | NO | | 都道府県名 |
| `length` | INT | NO | | トラック周長(メートル) |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### schedules テーブル
開催日程情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | VARCHAR(20) | NO | PK | スケジュールID (例: "2025031813_1") |
| `cup_id` | VARCHAR(15) | NO | FK | 開催ID |
| `date` | DATE | NO | | 開催日 |
| `day` | TINYINT | NO | | 開催日数の何日目か |
| `index` | TINYINT | NO | | 中止・延期を問わない何日目か |
| `entries_unfixed` | BOOLEAN | NO | | 出走表未確定フラグ |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### races テーブル
レース情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | VARCHAR(25) | NO | PK | レースID (例: "2025031813_1_1") |
| `cup_id` | VARCHAR(15) | NO | FK | 開催ID |
| `schedule_id` | VARCHAR(20) | NO | FK | スケジュールID |
| `number` | TINYINT | NO | | レース番号 |
| `name` | VARCHAR(100) | YES | | レース名 |
| `start_at` | INT | YES | | レース開始時刻(UNIXタイムスタンプ) |
| `distance` | INT | NO | | レース距離(メートル) |
| `lap` | TINYINT | NO | | 周回数 |
| `entries_number` | TINYINT | NO | | 出走選手数 |
| `class` | VARCHAR(20) | YES | | レースクラス |
| `race_type` | VARCHAR(20) | YES | | レース種別 |
| `race_type3` | VARCHAR(20) | YES | | レース種別（3区分） |
| `is_grade_race` | BOOLEAN | NO | | グレードレースフラグ |
| `status` | VARCHAR(20) | YES | | レースステータス |
| `weather` | VARCHAR(20) | YES | | 天候 |
| `wind_speed` | FLOAT | YES | | 風速 |
| `temperature` | FLOAT | YES | | 気温(℃) |
| `humidity` | FLOAT | YES | | 湿度(%) |
| `cancel` | BOOLEAN | NO | | レースキャンセルフラグ |
| `cancel_reason` | VARCHAR(100) | YES | | キャンセル理由 |
| `close_at` | INT | YES | | 締切時刻(UNIXタイムスタンプ) |
| `decided_at` | INT | YES | | 確定時刻(UNIXタイムスタンプ) |
| `has_digest_video` | BOOLEAN | NO | | ダイジェスト動画有無 |
| `digest_video` | VARCHAR(255) | YES | | ダイジェスト動画情報 |
| `digest_video_provider` | VARCHAR(50) | YES | | 動画プロバイダー |
| `winner_entry_id` | VARCHAR(30) | YES | FK | 勝者エントリーID |
| `winner_number` | TINYINT | YES | | 勝者の車番 |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### entries テーブル
レースの出走表情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | VARCHAR(30) | NO | PK | エントリーID (例: "2025031813_1_1_01") |
| `race_id` | VARCHAR(25) | NO | FK | レースID |
| `player_id` | VARCHAR(20) | NO | FK | 選手ID |
| `number` | TINYINT | NO | | 車番 |
| `class` | TINYINT | NO | | 選手クラス |
| `style` | TINYINT | NO | | 脚質番号 |
| `style_point` | FLOAT | YES | | 脚質指数 |
| `absent` | BOOLEAN | NO | | 欠場フラグ |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### players テーブル
選手情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | VARCHAR(20) | NO | PK | 選手ID |
| `name` | VARCHAR(100) | NO | | 選手名 |
| `name_short` | VARCHAR(50) | YES | | 選手名(短縮版) |
| `name_kana` | VARCHAR(100) | YES | | 選手名(カナ) |
| `age` | TINYINT | YES | | 年齢 |
| `birthday` | DATE | YES | | 生年月日 |
| `prefecture` | VARCHAR(50) | YES | | 所属都道府県 |
| `rank` | TINYINT | YES | | 選手ランク |
| `gear` | FLOAT | YES | | 標準ギア |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### records テーブル
レース結果の記録を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | INT | NO | PK,AUTO | レコードID |
| `race_id` | VARCHAR(25) | NO | FK | レースID |
| `player_id` | VARCHAR(20) | NO | FK | 選手ID |
| `standing` | TINYINT | NO | | 着順 |
| `time` | FLOAT | YES | | 走破タイム(秒) |
| `gear_ratio` | FLOAT | YES | | 使用ギア比 |
| `style` | VARCHAR(50) | YES | | 使用脚質 |
| `race_point` | FLOAT | YES | | レースポイント |
| `comment` | TEXT | YES | | コメント |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### オッズテーブル共通フィールド

全てのオッズテーブル（odds_win, odds_place, odds_bracketquinella, odds_bracketexacta, odds_quinella, odds_exacta, odds_trio, odds_trifecta）には以下の共通フィールドが含まれます：

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `id` | INT | NO | PK,AUTO | オッズID |
| `race_id` | VARCHAR(25) | NO | FK | レースID |
| `odds` | FLOAT | NO | | オッズ値 |
| `min_odds` | FLOAT | YES | | 最小オッズ値 |
| `max_odds` | FLOAT | YES | | 最大オッズ値 |
| `odds_str` | VARCHAR(10) | NO | | オッズ値の文字列表現 |
| `min_odds_str` | VARCHAR(10) | YES | | 最小オッズ値の文字列表現 |
| `max_odds_str` | VARCHAR(10) | YES | | 最大オッズ値の文字列表現 |
| `key` | VARCHAR(20) | YES | | キー文字列（組み合わせ識別用、APIからは配列または空文字の場合あり） |
| `type` | VARCHAR(20) | YES | | オッズタイプ識別子 |
| `popularity_order` | INT | YES | | 人気順位 |
| `unit_price` | INT | NO | | 単位金額（通常100円） |
| `payoff_unit_price` | INT | YES | | 払戻金額 |
| `is_final` | BOOLEAN | NO | | 最終オッズフラグ |
| `updated_at_ts` | INT | NO | | オッズ更新時刻(UNIXタイムスタンプ) |
| `payout_status` | TINYINT | NO | | 払い戻し状態(0:未払い, 1:払戻中, 2:払戻完了) |
| `created_at` | TIMESTAMP | NO | | 作成日時 |
| `updated_at` | TIMESTAMP | NO | | 更新日時 |

### odds_win テーブル
単勝オッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `number` | TINYINT | NO | | 車番 |
| `absent` | BOOLEAN | NO | | 欠場フラグ |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

### odds_place テーブル
ワイドオッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `number` | TINYINT | NO | | 車番 |
| `absent` | BOOLEAN | NO | | 欠場フラグ |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

### odds_bracketquinella テーブル
枠連オッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `bracket1` | TINYINT | NO | | 1枠目 |
| `bracket2` | TINYINT | NO | | 2枠目 |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

### odds_bracketexacta テーブル
枠単オッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `bracket1` | TINYINT | NO | | 1着枠 |
| `bracket2` | TINYINT | NO | | 2着枠 |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

### odds_quinella テーブル
二車連オッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `number1` | TINYINT | NO | | 1車番 |
| `number2` | TINYINT | NO | | 2車番 |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

### odds_exacta テーブル
二車単オッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `number1` | TINYINT | NO | | 1着車番 |
| `number2` | TINYINT | NO | | 2着車番 |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

### odds_trio テーブル
三連複オッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `number1` | TINYINT | NO | | 1車番 |
| `number2` | TINYINT | NO | | 2車番 |
| `number3` | TINYINT | NO | | 3車番 |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

### odds_trifecta テーブル
三連単オッズ情報を保存します。

| カラム | 型 | NULL | キー | 説明 |
|--------|----|----|-----|------|
| `number1` | TINYINT | NO | | 1着車番 |
| `number2` | TINYINT | NO | | 2着車番 |
| `number3` | TINYINT | NO | | 3着車番 |

※その他の共通フィールドは上記「オッズテーブル共通フィールド」を参照

## インデックス設計

### cups テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_cups_venue` (`venue_id`)
- INDEX `idx_cups_date` (`start_date`, `end_date`)
- INDEX `idx_cups_grade` (`grade`)

### venues テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_venues_name` (`name`)

### schedules テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_schedules_cup` (`cup_id`)
- INDEX `idx_schedules_date` (`date`)
- INDEX `idx_schedules_cup_index` (`cup_id`, `index`)

### races テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_races_cup` (`cup_id`)
- INDEX `idx_races_schedule` (`schedule_id`)
- INDEX `idx_races_schedule_number` (`schedule_id`, `number`)
- INDEX `idx_races_start` (`start_at`)
- INDEX `idx_races_status` (`status`)
- INDEX `idx_races_type` (`race_type`)

### entries テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_entries_race` (`race_id`)
- INDEX `idx_entries_player` (`player_id`)
- INDEX `idx_entries_race_number` (`race_id`, `number`)

### players テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_players_name` (`name`)
- INDEX `idx_players_prefecture` (`prefecture`)

### records テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_records_race` (`race_id`)
- INDEX `idx_records_player` (`player_id`)
- INDEX `idx_records_race_standing` (`race_id`, `standing`)
- UNIQUE INDEX `idx_records_race_player` (`race_id`, `player_id`)

### odds_win テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_win_race` (`race_id`)
- INDEX `idx_odds_win_race_number` (`race_id`, `number`)
- INDEX `idx_odds_win_updated` (`updated_at_ts`)

### odds_place テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_place_race` (`race_id`)
- INDEX `idx_odds_place_race_number` (`race_id`, `number`)
- INDEX `idx_odds_place_updated` (`updated_at_ts`)

### odds_bracketquinella テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_bquinella_race` (`race_id`)
- INDEX `idx_odds_bquinella_race_brackets` (`race_id`, `bracket1`, `bracket2`)
- INDEX `idx_odds_bquinella_updated` (`updated_at_ts`)

### odds_bracketexacta テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_bexacta_race` (`race_id`)
- INDEX `idx_odds_bexacta_race_brackets` (`race_id`, `bracket1`, `bracket2`)
- INDEX `idx_odds_bexacta_updated` (`updated_at_ts`)

### odds_quinella テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_quinella_race` (`race_id`)
- INDEX `idx_odds_quinella_race_numbers` (`race_id`, `number1`, `number2`)
- INDEX `idx_odds_quinella_updated` (`updated_at_ts`)

### odds_exacta テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_exacta_race` (`race_id`)
- INDEX `idx_odds_exacta_race_numbers` (`race_id`, `number1`, `number2`)
- INDEX `idx_odds_exacta_updated` (`updated_at_ts`)

### odds_trio テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_trio_race` (`race_id`)
- INDEX `idx_odds_trio_race_numbers` (`race_id`, `number1`, `number2`, `number3`)
- INDEX `idx_odds_trio_updated` (`updated_at_ts`)

### odds_trifecta テーブル
- PRIMARY KEY (`id`)
- INDEX `idx_odds_trifecta_race` (`race_id`)
- INDEX `idx_odds_trifecta_race_numbers` (`race_id`, `number1`, `number2`, `number3`)
- INDEX `idx_odds_trifecta_updated` (`updated_at_ts`)

## データベース更新パターン

### 開催情報の取得と保存
1. 月間開催一覧APIを呼び出し、venues テーブルと cups テーブルを更新
   - 競輪場情報（venues）と基本的な開催情報（cups）を保存
   - 開催一覧APIで取得した基本情報（id, name, startDate, endDate, duration, grade, venueId）を保存
   - 存在しない競輪場（venue）があれば venues テーブルに追加

2. 開催詳細APIを呼び出し、cups, schedules, races テーブルを更新
   - 開催詳細APIのレスポンスから取得したcup.labelsとcup.playersUnfixedを更新
   ```sql
   UPDATE cups SET 
     labels = JSON_ARRAY('F1', ...), 
     players_unfixed = false 
   WHERE id = '2025031813';
   ```
   
   - schedulesの情報を保存、既存の場合は更新
   ```sql
   INSERT INTO schedules (id, cup_id, date, day, index, entries_unfixed) 
   VALUES ('2025031813_1', '2025031813', '2025-03-18', 1, 1, false)
   ON DUPLICATE KEY UPDATE
     date = VALUES(date),
     day = VALUES(day),
     index = VALUES(index),
     entries_unfixed = VALUES(entries_unfixed);
   ```
   
   - racesの基本情報と詳細情報を保存
   ```sql
   INSERT INTO races (
     id, cup_id, schedule_id, number, name, start_at, 
     distance, lap, entries_number, class, race_type, race_type3,
     is_grade_race, status, weather, wind_speed, 
     cancel, cancel_reason, close_at, decided_at,
     has_digest_video, digest_video, digest_video_provider
   ) VALUES (
     '2025031813_1_1', '2025031813', '2025031813_1', 1, '初日特選', 1774183200,
     2000, 5, 9, 'A級', '予選', '予選',
     false, '確定', '晴れ', 2.0,
     false, NULL, 1774182200, 1774183800,
     false, NULL, NULL
   )
   ON DUPLICATE KEY UPDATE
     name = VALUES(name),
     start_at = VALUES(start_at),
     distance = VALUES(distance),
     lap = VALUES(lap),
     entries_number = VALUES(entries_number),
     class = VALUES(class),
     race_type = VALUES(race_type),
     race_type3 = VALUES(race_type3),
     is_grade_race = VALUES(is_grade_race),
     status = VALUES(status),
     weather = VALUES(weather),
     wind_speed = VALUES(wind_speed),
     cancel = VALUES(cancel),
     cancel_reason = VALUES(cancel_reason),
     close_at = VALUES(close_at),
     decided_at = VALUES(decided_at),
     has_digest_video = VALUES(has_digest_video),
     digest_video = VALUES(digest_video),
     digest_video_provider = VALUES(digest_video_provider);
   ```

### レース情報の取得と保存
1. レース詳細APIを呼び出し、races テーブルを更新
   - レース詳細APIからレースの詳細情報を更新
   - 特に race_type, status, weather, wind_speed などの最新情報を取得
   - 動画情報（has_digest_video, digest_video, digest_video_provider）を更新
   - レースがキャンセルされた場合は cancel=true とキャンセル理由を保存

2. 同時に players テーブルと entries テーブルも更新
   - レース詳細APIから取得した選手情報を players テーブルに保存または更新
   ```sql
   INSERT INTO players (id, name, name_short, name_kana, age, prefecture, rank)
   VALUES ('12345', '選手名', '選手', 'センシュメイ', 28, '東京', 3)
   ON DUPLICATE KEY UPDATE
     name = VALUES(name),
     name_short = VALUES(name_short),
     name_kana = VALUES(name_kana),
     age = VALUES(age),
     prefecture = VALUES(prefecture),
     rank = VALUES(rank);
   ```
   
   - 出走表情報を entries テーブルに保存
   ```sql
   INSERT INTO entries (id, race_id, player_id, number, class, style, style_point, absent)
   VALUES ('2025031813_1_1_01', '2025031813_1_1', '12345', 1, 3, 2, 85.5, false)
   ON DUPLICATE KEY UPDATE
     class = VALUES(class),
     style = VALUES(style),
     style_point = VALUES(style_point),
     absent = VALUES(absent);
   ```

3. レース終了後にレース詳細APIを再度呼び出し、races, records テーブルを更新
   - レース状態を更新（status = '確定'）
   - レース結果情報を records テーブルに登録
   ```sql
   INSERT INTO records (race_id, player_id, standing, time, gear_ratio, style, race_point, comment)
   VALUES ('2025031813_1_1', '12345', 1, 42.5, 4.53, 'マーク', 120.5, 'コメント')
   ON DUPLICATE KEY UPDATE
     standing = VALUES(standing),
     time = VALUES(time),
     gear_ratio = VALUES(gear_ratio),
     style = VALUES(style),
     race_point = VALUES(race_point),
     comment = VALUES(comment);
   ```
   
   - 勝者情報をraces テーブルに更新
   ```sql
   UPDATE races SET
     winner_entry_id = '2025031813_1_1_01',
     winner_number = 1,
     decided_at = 1774183800
   WHERE id = '2025031813_1_1';
   ```

### オッズ情報の取得と保存
1. オッズAPIを呼び出し、各種オッズテーブルを更新
   - 単勝オッズ（odds_win）の更新
   ```sql
   INSERT INTO odds_win (
     race_id, number, absent, odds, odds_str, popularity_order, 
     is_final, updated_at_ts, payout_status
   ) VALUES (
     '2025031813_1_1', 1, false, 3.5, '3.5', 1,
     false, 1774179600, 0
   )
   ON DUPLICATE KEY UPDATE
     odds = VALUES(odds),
     odds_str = VALUES(odds_str),
     popularity_order = VALUES(popularity_order),
     is_final = VALUES(is_final),
     updated_at_ts = VALUES(updated_at_ts),
     payout_status = VALUES(payout_status);
   ```
   
   - 同様に他のオッズ種別（place, quinella, exacta, trio, trifecta）も更新
   - 毎回最新のオッズ情報で上書き更新

2. 最終オッズで更新されるまで定期的に呼び出し
   - オッズAPIのレスポンスで is_final=true を確認したら更新を停止
   - 払戻情報も available になったら payout_status を更新

### レースステータスに応じたデータ更新フロー

#### 1. レース前（status = '予定'）
- 基本レース情報の取得と保存
- 出走表情報の更新
- オッズ情報の定期的な更新

#### 2. レース中（status = '発走'）
- レースステータスの更新
- オッズの最終値の保存（is_final = true）

#### 3. レース終了後（status = '確定'）
- レース結果情報の取得と保存
- 払戻情報の更新
- 動画情報の更新（利用可能になった場合）

#### 4. レースキャンセル時（cancel = true）
- キャンセル理由の保存
- 関連するオッズ情報の更新（払戻処理等）

### エラーハンドリング
- API接続エラー時はリトライ処理を実装
- データ整合性エラーが発生した場合はログを記録し、手動確認を促す仕組み
- クリティカルな更新（結果確定など）に失敗した場合はアラート通知

### 更新処理の実装例（疑似コード）

```python
def update_cup_data(cup_id):
    # 開催詳細APIを呼び出し
    cup_detail = api_client.get_cup_detail(cup_id)
    
    # cupsテーブルの更新
    update_cup_info(cup_detail['cup'])
    
    # schedulesテーブルの更新
    for schedule in cup_detail.get('schedules', []):
        update_schedule_info(schedule)
    
    # racesテーブルの更新
    for race in cup_detail.get('races', []):
        update_race_info(race)
    
    return True

def update_race_data(race_id):
    # レース詳細APIを呼び出し
    race_detail = api_client.get_race_detail(race_id)
    
    # racesテーブルの更新
    update_race_info(race_detail['race'])
    
    # entriesテーブルとplayersテーブルの更新
    for entry in race_detail.get('entries', []):
        update_player_info(entry['player'])
        update_entry_info(race_id, entry)
    
    # レースが終了している場合は結果も更新
    if race_detail['race']['status'] == '確定':
        update_race_result(race_id, race_detail)
    
    return True

def update_odds_data(race_id):
    # オッズAPIを呼び出し
    odds_data = api_client.get_odds(race_id)
    
    # 各種オッズの更新
    update_win_odds(race_id, odds_data.get('win', []))
    update_place_odds(race_id, odds_data.get('place', []))
    update_quinella_odds(race_id, odds_data.get('quinella', []))
    update_exacta_odds(race_id, odds_data.get('exacta', []))
    update_trio_odds(race_id, odds_data.get('trio', []))
    update_trifecta_odds(race_id, odds_data.get('trifecta', []))
    
    # 最終オッズ確認
    is_final = odds_data.get('isFinal', False)
    
    return is_final
```

## データ更新頻度の推奨

| 情報タイプ | 推奨更新頻度 |
|------------|------------|
| 開催情報・開催詳細 | 1日1回 |
| レース情報 | レース開始2時間前、30分前、5分前 |
| オッズ情報 | 5分間隔（最終オッズまで） |
| レース結果 | レース終了5分後 |

## サンプルクエリ

### 特定開催のレース一覧取得
```sql
SELECT r.* 
FROM races r
JOIN schedules s ON r.schedule_id = s.id
WHERE s.cup_id = '2025031813'
ORDER BY s.index, r.number;
```

### 特定レースの出走表と選手情報取得
```sql
SELECT e.number, p.name, p.prefecture, e.style, e.absent
FROM entries e
JOIN players p ON e.player_id = p.id
WHERE e.race_id = '2025031813_1_1'
ORDER BY e.number;
```

### 特定レースの結果取得
```sql
SELECT r.standing, p.name, p.prefecture, r.time, r.style, r.race_point
FROM records r
JOIN players p ON r.player_id = p.id
WHERE r.race_id = '2025031813_1_1'
ORDER BY r.standing;
```

### 特定レースの単勝オッズ取得
```sql
SELECT number, odds, is_final
FROM odds_win
WHERE race_id = '2025031813_1_1'
ORDER BY number;
```

### 特定レースの二車連オッズ取得
```sql
SELECT number1, number2, odds, is_final
FROM odds_quinella
WHERE race_id = '2025031813_1_1'
ORDER BY odds;
```

### 特定レースの三連単オッズ取得
```sql
SELECT number1, number2, number3, odds, is_final
FROM odds_trifecta
WHERE race_id = '2025031813_1_1'
ORDER BY odds;
```

## 注意事項
- プライマリキーは、APIから取得したIDをそのまま使用する設計になっています
- 実際のデータベース実装では、使用するDBMSに応じて型や制約を適切に変更してください
- 大量データを扱う場合は、過去データのアーカイブ戦略を検討してください
- 選手情報は変更頻度が低いため、players テーブルは徐々に蓄積されていきます 