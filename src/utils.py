from pyspark.sql import SparkSession

def init_spark():
    """
    This ``function`` reads tweets json file and process it to compute the first 10 used emojis with their
    respective count. This, in terms of time performance.

        Args:
            None

        Returns:
            spark (SparkSession): spark session which will allow us to process our data
    """
    spark = SparkSession.builder.appName('latam_challenge').getOrCreate()

    return spark