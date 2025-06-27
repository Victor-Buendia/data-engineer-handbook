from pyspark.sql.functions import (
    col,
    when,
    avg,
    lit,
    collect_list,
    struct,
    concat,
    coalesce,
)
from pyspark.sql import SparkSession

# bootcamp-solutions/1-dimensional-data-modeling/homework/2_cumulative.sql
def do_cumulative_transformation(dfs, year):
    actors_df, actor_films_df = dfs
    yesterday_df = actors_df.where(col("year") == lit(year)).select(
        "actorid", "actor", "films", "quality_class", "is_active", "year"
    )
    today_df = (
        actor_films_df.where(col("year") == lit(year + 1))
        .groupby("actorid", "actor", "year")
        .agg(
            collect_list(
                struct(
                    col("film"), col("votes"), col("year"), col("rating"), col("filmid")
                )
            ).alias("films"),
            avg(col("rating")).alias("avg_rating"),
        )
        .withColumn(
            "quality_class",
            when(col("avg_rating") > 8, "star")
            .when(col("avg_rating") > 7, "good")
            .when(col("avg_rating") > 6, "average")
            .otherwise("bad"),
        )
    )
    actors_data_df = (
        yesterday_df.alias("y")
        .join(today_df.alias("t"), col("y.actorid") == col("t.actorid"), "full_outer")
        .select(
            coalesce(col("t.actorid"), col("y.actorid")).alias("actorid"),
            coalesce(col("t.actor"), col("y.actor")).alias("actor"),
            when(col("t.actor").isNull(), col("y.films"))
            .when(col("y.actor").isNull(), col("t.films"))
            .otherwise(concat(col("t.films"), col("y.films")))
            .alias("films"),
            coalesce(col("t.quality_class"), col("y.quality_class")).alias(
                "quality_class"
            ),
            col("t.actor").isNotNull().alias("is_active"),
            coalesce(col("t.year"), col("y.year") + 1).alias("year"),
        )
    )
    return actors_data_df

def main():
    spark = SparkSession.builder.master("local").appName("cumulative_table_design_job").getOrCreate()
    output_df = do_cumulative_transformation(dfs=(spark.table("actors_cumulative"), spark.table("actor_films")), year=2023)
    output_df.write.mode("append").insertInto("actors_cumulative")

