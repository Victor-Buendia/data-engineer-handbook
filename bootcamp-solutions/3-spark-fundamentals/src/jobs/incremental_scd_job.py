from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession

columns_order = [
    "actorid",
    "actor",
    "quality_class",
    "is_active",
    "start_date",
    "end_date",
    "year",
]

# bootcamp-solutions/1-dimensional-data-modeling/homework/5_scq.sql
def do_incremental_scd_transformation(dfs, current_year):
    actors_df, history_df = dfs

    historical_data = history_df.filter(
        (F.col("year") == F.lit(current_year-1))
        & (F.col("end_date") < F.lit(current_year-1))
    ).withColumn("year", F.lit(current_year).cast(T.LongType()))

    last_year_data = history_df.filter(
        (F.col("year") == F.lit(current_year-1))
        & (F.col("end_date") == F.lit(current_year-1))
    )

    this_year_data = actors_df.filter(
        F.col("year") == F.lit(current_year)
    )

    # SCD Logic
    unchanged_records_filter = (
        (F.col("ly.actorid") == F.col("ty.actorid"))
        & (F.col("ly.quality_class") == F.col("ty.quality_class"))
        & (F.col("ly.is_active") == F.col("ty.is_active"))
    )
    unchanged_records = (
        last_year_data.alias("ly").join(this_year_data.alias("ty"), unchanged_records_filter, "inner")
        .select(
            F.col("ly.actorid"),
            F.col("ly.actor"),
            F.col("ly.quality_class"),
            F.col("ly.is_active"),
            F.col("ly.start_date").alias("start_date"),
            F.lit(current_year).alias("end_date"),
            F.lit(current_year).alias("year"),
        )
    )
    
    changed_records_filter = (
        (F.col("ly.actorid") == F.col("ty.actorid")) & 
        (
            (F.col("ly.quality_class") != F.col("ty.quality_class")) |
            (F.col("ly.is_active") != F.col("ty.is_active"))
        )
    )
    changed_records = (
        last_year_data.alias("ly").join(
            this_year_data.alias("ty"),
            changed_records_filter,
            "inner"
        )
        .select(
            F.col("ly.actorid"),
            F.col("ly.actor"),
            F.explode(
                F.array(
                    F.struct(
                        F.col("ty.quality_class").alias("quality_class"),
                        F.col("ty.is_active").alias("is_active"),
                        F.lit(current_year).alias("year"),
                        F.lit(current_year).alias("start_date"),
                        F.lit(current_year).alias("end_date"),
                    ),
                    F.struct(
                        F.col("ly.quality_class").alias("quality_class"),
                        F.col("ly.is_active").alias("is_active"),
                        F.lit(current_year).alias("year"),
                        F.col("start_date").alias("start_date"),
                        F.col("end_date").alias("end_date"),
                    ),
                )
            ).alias("quality_class_change")
        )
        .select(
            F.col("ly.actorid"),
            F.col("ly.actor"),
            F.col("quality_class_change.quality_class"),
            F.col("quality_class_change.is_active"),
            F.col("quality_class_change.start_date"),
            F.col("quality_class_change.end_date"),
            F.col("quality_class_change.year")
        )
    )

    new_records = (
        this_year_data.alias("ty")
        .join(
            last_year_data.alias("ly"),
            F.col("ty.actorid") == F.col("ly.actorid"),
            "left_anti"
        )
        .select(
            F.col("ty.actorid"),
            F.col("ty.actor"),
            F.col("ty.quality_class"),
            F.col("ty.is_active"),
            F.lit(current_year).alias("year"),
            F.lit(current_year).alias("start_date"),
            F.lit(current_year).alias("end_date")
        )
    )

    final_df = (
        historical_data.select(*columns_order)
        .unionAll(unchanged_records.select(*columns_order))
        .unionAll(new_records.select(*columns_order))
        .unionAll(changed_records.select(*columns_order))
    )

    return final_df

def main():
    spark = SparkSession.builder.master("local").appName("cumulative_table_design_job").getOrCreate()
    output_df = do_incremental_scd_transformation(dfs=(spark.table("actors_cumulative"), spark.table("actors_incremental_scd")), year=2023)
    output_df.write.mode("append").insertInto("actors_incremental_scd")
