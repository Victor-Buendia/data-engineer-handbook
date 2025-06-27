from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from ..jobs.incremental_scd_job import do_incremental_scd_transformation

ActorSCD = namedtuple(
    "ActorSCD",
    [
        "actorid",
        "actor",
        "quality_class",
        "is_active",
        "start_date",
        "end_date",
        "year",
    ],
)
Actor = namedtuple("Actor", ["actorid", "actor", "quality_class", "is_active", "year"])


def test_incremental_scd_job(spark):
    actors_history_scd_df = spark.createDataFrame(
        [
            ActorSCD(1, "Actor 1", "star", False, 2021, 2022, 2022),
            ActorSCD(2, "Actor 2", "good", False, 2020, 2022, 2022),
            ActorSCD(3, "Actor 3", "bad", True, 2022, 2022, 2022),
            ActorSCD(1, "Actor 1", "star", True, 2019, 2020, 2022),
        ]
    )
    actors_df = spark.createDataFrame(
        [
            Actor(1, "Actor 1", "star", False, 2023),
            Actor(2, "Actor 2", "average", True, 2023),
            Actor(3, "Actor 3", "bad", True, 2023),
            Actor(4, "Actor 4", "star", True, 2023),
        ]
    )
    actual_df = do_incremental_scd_transformation(
        spark, (actors_df, actors_history_scd_df), 2023
    )
    expected_df = spark.createDataFrame(
        [
            # Historical records from previous year
            ActorSCD(1, "Actor 1", "star", True, 2019, 2020, 2023),
            # Unchanged records from last year
            ActorSCD(1, "Actor 1", "star", False, 2021, 2023, 2023),
            ActorSCD(3, "Actor 3", "bad", True, 2022, 2023, 2023),
            # New record for this year
            ActorSCD(4, "Actor 4", "star", True, 2023, 2023, 2023),
            # Changed records from last year
            ActorSCD(2, "Actor 2", "average", True, 2023, 2023, 2023),
            ActorSCD(2, "Actor 2", "good", False, 2020, 2022, 2023),
        ]
    )
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
