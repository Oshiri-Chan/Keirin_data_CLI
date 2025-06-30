    DROP TABLE IF EXISTS odds_quinella_place;
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