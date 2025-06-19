CREATE TABLE actors_history_scd (
    actorid TEXT,
    actor TEXT,
    quality_class quality_class,
    is_active BOOL,
    start_date INT,
    end_date INT,
    year INT,
    PRIMARY KEY (actorid, start_date, year)
);