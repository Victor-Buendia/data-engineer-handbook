from collections import namedtuple
from chispa.dataframe_comparer import assert_df_equality

# Import your function here
# from your_module import do_cumulative_transformation
from ..jobs.cumulative_table_job import do_cumulative_transformation

Film = namedtuple("Film", ["film", "votes", "year", "rating", "filmid"])
Actor = namedtuple(
    "Actor", ["actorid", "actor", "films", "quality_class", "is_active", "year"]
)
ActorFilm = namedtuple(
    "ActorFilm", ["actorid", "actor", "film", "votes", "year", "rating", "filmid"]
)
ActorCumulative = namedtuple(
    "ActorCumulative",
    ["actorid", "actor", "films", "quality_class", "is_active", "year"],
)


def test_do_cumulative_transformation(spark):
    # Prepare input data using namedtuples
    actors_data = [
        Actor(1, "Actor A", [Film("Film1", 100, 2022, 7.5, 10)], "good", True, 2022),
        Actor(2, "Actor B", [], "bad", True, 2022),
    ]

    actor_films_data = [
        ActorFilm(1, "Actor A", "Film2", 150, 2023, 8.2, 11),
        ActorFilm(1, "Actor A", "Film3", 200, 2023, 6.5, 12),
        ActorFilm(2, "Actor B", "Film4", 120, 2023, 6.1, 13),
        ActorFilm(3, "Actor C", "Film5", 500, 2023, 2.1, 15),
    ]

    expected_data = [
        ActorCumulative(
            1,
            "Actor A",
            [
                Film("Film2", 150, 2023, 8.2, 11),
                Film("Film3", 200, 2023, 6.5, 12),
                Film("Film1", 100, 2022, 7.5, 10),
            ],
            "good",
            True,
            2023,
        ),
        ActorCumulative(
            2,
            "Actor B",
            [
                Film("Film4", 120, 2023, 6.1, 13),
            ],
            "average",
            True,
            2023,
        ),
        ActorCumulative(
            3,
            "Actor C",
            [
                Film("Film5", 500, 2023, 2.1, 15),
            ],
            "bad",
            True,
            2023,
        ),
    ]

    actors_df = spark.createDataFrame(actors_data)
    actor_films_df = spark.createDataFrame(actor_films_data)
    expected_df = spark.createDataFrame(expected_data)

    # Run the function under test
    result_df = do_cumulative_transformation(spark, (actors_df, actor_films_df), 2022)

    # Assert the result
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
