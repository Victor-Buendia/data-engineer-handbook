CREATE TYPE films AS (
    film TEXT,
    votes INT,
    year INT,
    rating FLOAT,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

CREATE TABLE IF NOT EXISTS actors (
    actorid TEXT,
    actor TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOL,
    year INT,
    PRIMARY KEY (actorid, year)
);