# Output data type
from typing import List, Tuple

# From utils
from utils import init_spark

# To handle emojis from content
import emoji

# Transformation
from pyspark.sql.functions import col, desc, size, expr, explode, filter

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    This ``function`` reads tweets json file and process it to compute the first 10 used emojis with their
    respective count. This, in terms of time performance.

        Args:
            file_path (str): Tweet JSON file which contains tweets from several days.

        Returns:
            q2_time_lst (List): List of tuples containing the result from data processing.
    """
    spark = init_spark()

    tweet_list_df = spark.read.json(file_path)
    
    re_pattern = r'[\u0080-\uffff\U00010000-\U0001ffff]'
    to_compare = list(emoji.unicode_codes.EMOJI_DATA.keys())
    
    is_emoji = lambda x: x.isin(to_compare)

    tweet_list_df = tweet_list_df.coalesce(4)
    
    tweet_emoji_df = tweet_list_df \
    .withColumn('emoji_lst', expr(f"regexp_extract_all(content, '{re_pattern}', 0)")) \
    .filter(size(col('emoji_lst'))!=0) \
    .select('emoji_lst') \
    .withColumn('emoji_new',filter('emoji_lst', is_emoji)) \
    .filter(size(col('emoji_new'))!=0) \
    .select(explode('emoji_new').alias('emoji_content')) \
    .groupBy("emoji_content") \
    .count() \
    .sort(desc("count")) \
    .limit(10)

    q2_time_lst = tweet_emoji_df \
        .rdd.map(lambda x: (x[0], x[1])) \
        .collect()
    
    return q2_time_lst