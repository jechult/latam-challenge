# Output data type
from typing import List, Tuple
from datetime import datetime

# From utils
from utils import init_spark

# Transformation
from pyspark.sql.functions import to_date, col, row_number, desc
from pyspark.sql.window import Window

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    This ``function`` reads tweets json file and process it to compute the first 10 dates with most tweets
    and their respective user with the most tweets. This, in terms of time performance.

        Args:
            file_path (str): Tweet JSON file which contains tweets from several days.

        Returns:
            q1_time_lst (List): List of tuples containing the result from data processing.
    """
    spark = init_spark()

    tweet_list_df = spark.read.json(file_path)

    tweet_list_df = tweet_list_df.coalesce(4)
    
    tweet_list_df = tweet_list_df \
    .withColumn("parsed_date", to_date("date")) \
    .select(col("parsed_date").alias("tweet_dt"),col("user.username").alias("user_name"))
    
    tweet_grp_df = tweet_list_df \
    .groupBy("tweet_dt") \
    .count() \
    .sort(desc("count")) \
    .limit(10)
    
    tweet_usr_grp_df = tweet_list_df \
    .join(tweet_grp_df, "tweet_dt", "inner") \
    .groupBy("tweet_dt", "user_name")\
    .count() \
    .withColumn("rank_num", row_number().over(Window.partitionBy("tweet_dt").orderBy(desc("count")))) \
    .filter(col("rank_num") == 1) \
    .select("tweet_dt","user_name")

    q1_time_lst = tweet_usr_grp_df \
        .rdd.map(lambda x: (x[0], x[1])) \
        .collect()
    
    return q1_time_lst