-- MySQL用のテーブル作成スキーマ (IDはVARCHAR)

-- -----------------------------------------------------
-- Step 1: 開催情報
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS regions (
    region_id VARCHAR(2) PRIMARY KEY, -- 実データ最大長: 1
    region_name VARCHAR(5) NOT NULL, -- 実データ最大長: 3
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS venues (
    venue_id VARCHAR(3) PRIMARY KEY, -- 実データ最大長: 2
    venue_name VARCHAR(10) NOT NULL, -- 実データ最大長: 4
    name1 VARCHAR(20), -- 実データ最大長: 1
    address TEXT, -- 実データ最大長: 23
    phoneNumber VARCHAR(30), -- 実データ最大長: 12
    websiteUrl VARCHAR(2083), -- 実データ最大長: 49
    bankFeature TEXT, -- 実データ最大長: 269
    trackStraightDistance DOUBLE,
    trackAngleCenter VARCHAR(255), -- 実データ最大長: 9
    trackAngleStraight VARCHAR(255), -- 実データ最大長: 16
    homeWidth INT,
    backWidth INT,
    centerWidth DOUBLE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS cups (
    cup_id VARCHAR(15) PRIMARY KEY, -- 実データ最大長: 10
    cup_name VARCHAR(30) NOT NULL, -- 実データ最大長: 15
    start_date DATE,
    end_date DATE,
    duration INT,
    grade INT,
    venue_id VARCHAR(3), -- FK: venues.venue_id
    labels TEXT, -- 実データ最大長: 9
    players_unfixed BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (venue_id) REFERENCES venues(venue_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------
-- Step 2: レース情報, スケジュール情報, 処理ステータス
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS schedules (
    schedule_id VARCHAR(10) PRIMARY KEY, -- 実データ最大長: 10
    cup_id VARCHAR(15) NOT NULL, -- FK: cups.cup_id
    `date` DATE,
    day INT,
    entries_unfixed BOOLEAN DEFAULT FALSE,
    schedule_index INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (cup_id) REFERENCES cups(cup_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS races (
    race_id VARCHAR(12) PRIMARY KEY, -- 実データ最大長: 12
    schedule_id VARCHAR(10), -- FK: schedules.schedule_id
    cup_id VARCHAR(15), -- FK: cups.cup_id
    number INT,
    `class` VARCHAR(10), -- 実データ最大長: 4
    race_type VARCHAR(20), -- 実データ最大長: 16
    start_at BIGINT, 
    close_at BIGINT, 
    `status` INT,
    cancel BOOLEAN DEFAULT FALSE,
    cancel_reason TEXT, -- 実データ最大長: 5
    weather VARCHAR(5), -- 実データ最大長: 2
    wind_speed VARCHAR(10), -- 実データ最大長: 4
    race_type3 VARCHAR(5), -- 実データ最大長: 3
    distance INT,
    lap INT,
    entries_number INT,
    is_grade_race BOOLEAN DEFAULT FALSE,
    has_digest_video BOOLEAN DEFAULT FALSE,
    digest_video TEXT, -- 実データ最大長: 67
    digest_video_provider INT,
    decided_at BIGINT, 
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (cup_id) REFERENCES cups(cup_id) ON DELETE SET NULL,
    FOREIGN KEY (schedule_id) REFERENCES schedules(schedule_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS race_status (
    race_id VARCHAR(12) PRIMARY KEY, -- FK: races.race_id
    step3_status VARCHAR(10) DEFAULT 'pending', -- 実データ最大長: 10
    step4_status VARCHAR(10) DEFAULT 'pending', -- 実データ最大長: 10
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------
-- Step 3: 選手・出走情報
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS players (
    race_id VARCHAR(12) NOT NULL, -- 実データ最大長: 12
    player_id VARCHAR(6) NOT NULL, -- 実データ最大長: 5 (entriesも考慮し6)
    name VARCHAR(10), -- 実データ最大長: 6
    `class` VARCHAR(5), -- 実データ最大長: 1
    player_group VARCHAR(5), -- 実データ最大長: 1
    prefecture VARCHAR(5), -- 実データ最大長: 3
    term INT,
    region_id VARCHAR(2), -- FK: regions.region_id
    yomi VARCHAR(15), -- 実データ最大長: 13
    birthday DATE,
    age INT,
    gender INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, player_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id) ON DELETE SET NULL,
    INDEX idx_player_id (player_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS entries (
    number INT NOT NULL,
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    absent BOOLEAN DEFAULT FALSE,
    player_id VARCHAR(6), -- FK: players.player_id
    bracket_number INT,
    player_current_term_class INT,
    player_current_term_group INT,
    player_previous_term_class INT,
    player_previous_term_group INT,
    has_previous_class_group BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, number),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE SET NULL 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS line_predictions (
    race_id VARCHAR(12) NOT NULL PRIMARY KEY, -- FK: races.race_id
    line_type VARCHAR(5), -- 実データ最大長: 3
    line_formation VARCHAR(30), -- 実データ最大長: 21
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS player_records (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    player_id VARCHAR(6), -- FK: players.player_id (NULLを許容するように変更)
    gear_ratio DOUBLE,
    style VARCHAR(5), -- 実データ最大長: 1
    race_point DOUBLE,
    comment VARCHAR(30), -- 実データ最大長: 22
    prediction_mark INT,
    first_rate DOUBLE,
    second_rate DOUBLE,
    third_rate DOUBLE,
    has_modified_gear_ratio BOOLEAN DEFAULT FALSE,
    modified_gear_ratio DOUBLE,
    modified_gear_ratio_str VARCHAR(5), -- 実データ最大長: 4
    gear_ratio_str VARCHAR(5), -- 実データ最大長: 4
    race_point_str VARCHAR(10), -- 実データ最大長: 6
    previous_cup_id VARCHAR(15), -- FK: cups.cup_id
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, player_id),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE,
    FOREIGN KEY (previous_cup_id) REFERENCES cups(cup_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------
-- Step 4: オッズ情報
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS odds_statuses (
    race_id VARCHAR(12) PRIMARY KEY, -- FK: races.race_id
    trifecta_payoff_status INT,
    trio_payoff_status INT,
    exacta_payoff_status INT,
    quinella_payoff_status INT,
    quinella_place_payoff_status INT,
    bracket_exacta_payoff_status INT,
    bracket_quinella_payoff_status INT,
    is_aggregated BOOLEAN,
    odds_updated_at_timestamp BIGINT,
    odds_delayed BOOLEAN,
    final_odds BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS odds_trifecta (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    `type` INT NOT NULL, 
    `key` VARCHAR(9) NOT NULL, -- 実データ最大長: 9
    odds DOUBLE,
    min_odds DOUBLE,
    max_odds DOUBLE,
    odds_str VARCHAR(6), -- 実データ最大長: 6
    min_odds_str VARCHAR(3), -- 実データ最大長: 3
    max_odds_str VARCHAR(3), -- 実データ最大長: 3
    popularity_order INT,
    unit_price INT,
    payoff_unit_price INT,
    absent BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, `key`), 
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS odds_trio (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    `type` INT NOT NULL,
    `key` VARCHAR(9) NOT NULL, -- 実データ最大長: 9
    odds DOUBLE,
    min_odds DOUBLE,
    max_odds DOUBLE,
    odds_str VARCHAR(6), -- 実データ最大長: 6
    min_odds_str VARCHAR(3), -- 実データ最大長: 3
    max_odds_str VARCHAR(3), -- 実データ最大長: 3
    popularity_order INT,
    unit_price INT,
    payoff_unit_price INT,
    absent BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, `key`),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS odds_exacta (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    `type` INT NOT NULL,
    `key` VARCHAR(9) NOT NULL, -- 実データ最大長: 9
    odds DOUBLE,
    min_odds DOUBLE,
    max_odds DOUBLE,
    odds_str VARCHAR(6), -- 実データ最大長: 6
    min_odds_str VARCHAR(3), -- 実データ最大長: 3
    max_odds_str VARCHAR(3), -- 実データ最大長: 3
    popularity_order INT,
    unit_price INT,
    payoff_unit_price INT,
    absent BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, `key`),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS odds_quinella (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    `type` INT NOT NULL,
    `key` VARCHAR(9) NOT NULL, -- 実データ最大長: 9
    odds DOUBLE,
    min_odds DOUBLE,
    max_odds DOUBLE,
    odds_str VARCHAR(6), -- 実データ最大長: 6
    min_odds_str VARCHAR(3), -- 実データ最大長: 3
    max_odds_str VARCHAR(3), -- 実データ最大長: 3
    popularity_order INT,
    unit_price INT,
    payoff_unit_price INT,
    absent BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, `key`),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS odds_quinella_place (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    `type` INT NOT NULL,
    `key` VARCHAR(9) NOT NULL, -- 実データ最大長: 9
    odds DOUBLE,
    min_odds DOUBLE,
    max_odds DOUBLE,
    odds_str VARCHAR(6), -- 実データ最大長: 6
    min_odds_str VARCHAR(3), -- 実データ最大長: 3
    max_odds_str VARCHAR(6), -- 実データ最大長: 3 -> 6 に修正
    popularity_order INT,
    unit_price INT,
    payoff_unit_price INT,
    absent BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, `key`),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS odds_bracket_exacta (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    `type` INT NOT NULL,
    `key` VARCHAR(9) NOT NULL, -- 実データ最大長: 9
    odds DOUBLE,
    min_odds DOUBLE,
    max_odds DOUBLE,
    odds_str VARCHAR(6), -- 実データ最大長: 6
    min_odds_str VARCHAR(3), -- 実データ最大長: 3
    max_odds_str VARCHAR(3), -- 実データ最大長: 3
    popularity_order INT,
    unit_price INT,
    payoff_unit_price INT,
    absent BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, `key`),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS odds_bracket_quinella (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    `type` INT NOT NULL,
    `key` VARCHAR(9) NOT NULL, -- 実データ最大長: 9
    odds DOUBLE,
    min_odds DOUBLE,
    max_odds DOUBLE,
    odds_str VARCHAR(6), -- 実データ最大長: 6
    min_odds_str VARCHAR(3), -- 実データ最大長: 3
    max_odds_str VARCHAR(3), -- 実データ最大長: 3
    popularity_order INT,
    unit_price INT,
    payoff_unit_price INT,
    absent BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, `key`),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------
-- Step 5: 結果情報
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS race_results (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    bracket_number INT NOT NULL,
    `rank` INT,
    rank_text VARCHAR(1), -- 実データ最大長: 1
    mark VARCHAR(1), -- 実データ最大長: 1
    player_name VARCHAR(6), -- 実データ最大長: 6
    player_id VARCHAR(6), -- FK: players.player_id, 実データ最大長: 5
    age INT,
    prefecture VARCHAR(3), -- 実データ最大長: 3
    period INT,
    `class` VARCHAR(2), -- 実データ最大長: 2
    diff VARCHAR(5), -- 実データ最大長: 5
    `time` DOUBLE,
    last_lap_time VARCHAR(4), -- 実データ最大長: 4
    winning_technique VARCHAR(3), -- 実データ最大長: 3
    symbols VARCHAR(4), -- 実データ最大長: 4
    win_factor VARCHAR(6), -- 実データ最大長: 6
    personal_status VARCHAR(80), -- 実データ最大長: 79
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, bracket_number),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS race_comments (
    race_id VARCHAR(12) PRIMARY KEY, -- FK: races.race_id
    comment VARCHAR(36), -- 実データ最大長: 36
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS inspection_reports (
    race_id VARCHAR(12) NOT NULL, -- FK: races.race_id
    player VARCHAR(6) NOT NULL, -- player_id を想定 (データ無) 
    comment TEXT, -- データ無
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (race_id, player),
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS lap_positions (
    race_id VARCHAR(12) PRIMARY KEY, -- FK: races.race_id
    lap_shuukai VARCHAR(300), -- 実データ最大長: 245
    lap_akaban VARCHAR(300), -- 実データ最大長: 249
    lap_dasho VARCHAR(300), -- 実データ最大長: 248
    lap_hs VARCHAR(300), -- 実データ最大長: 245
    lap_bs VARCHAR(300), -- 実データ最大長: 223
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS lap_data_status (
    race_id VARCHAR(12) PRIMARY KEY, -- FK: races.race_id
    is_processed BOOLEAN DEFAULT FALSE,
    last_checked_at DATETIME,
    FOREIGN KEY (race_id) REFERENCES races(race_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;