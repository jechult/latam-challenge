from typing import List, Tuple

# From utils
from utils import init_spark

# Transformation
from pyspark.sql.functions import col, desc, explode

from memory_profiler import profile

file_path = "farmers-protest-tweets-2021-2-4.json"

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    This ``function`` reads tweets json file and process it to compute the first 10 influential users with their
    respective count. This, in terms of memory performance.

        Args:
            file_path (str): Tweet JSON file which contains tweets from several days.

        Returns:
            q3_memory_lst (List): List of tuples containing the result from data processing.
    """
    spark = init_spark()

    tweet_list_df = spark.read.json(file_path)
    
    tweet_list_df.cache().count()
        
    tweet_top_usr_df = tweet_list_df \
    .select('mentionedUsers.username') \
    .filter(col('username').isNotNull()) \
    .select(explode('username').alias('user_name')) \
    .groupBy('user_name') \
    .count() \
    .sort(desc('count')) \
    .limit(10)
    
    tweet_list_df.unpersist().count()
    
    q3_memory_lst = tweet_top_usr_df \
        .rdd.map(lambda x: (x[0], x[1])) \
        .collect()
    
    return q3_memory_lst

if __name__ == '__main__':
    q3_memory(file_path)