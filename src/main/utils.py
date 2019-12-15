import logging
from pyspark.sql import SparkSession


class Utils:
    """
    This class provides any shared utilities or helper functions such as logging
    """

    @staticmethod
    def get_spark_session():
        """ 
        Returns the spark session
        """
        spark = SparkSession.builder.appName('icg_tech_test')\
                            .appName("Word Count").getOrCreate()
        # .master("local")
        # .config("spark.some.config.option", "some-value")
        return spark

    @staticmethod
    def get_logger():

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s:%(levelname)s:%(message)s',
                            level=logging.INFO)

