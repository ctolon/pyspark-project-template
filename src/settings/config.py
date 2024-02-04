
from functools import lru_cache

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, FloatType

DEFAULT_SHUFFLE_PARTITIONS: str = "24"
DEFAULT_MAX_RESULT_SIZE: str = "0"


def spark_conf_default(
    shuffle_partitions: str = DEFAULT_SHUFFLE_PARTITIONS,
) -> SparkConf:
    return (
        SparkConf()
        .set("spark.driver.maxResultsSize", DEFAULT_MAX_RESULT_SIZE)
        .set("spark.sql.shuffle.partitions", shuffle_partitions)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
        .set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    )
    
@lru_cache(maxsize=None)
def get_spark_session(spark_conf: SparkConf = spark_conf_default(), app_name: str = "spark_app") -> SparkSession:
    """
    Builds a spark session object
    For more information about configuration properties, see https://spark.apache.org/docs/latest/configuration.html
    :return: a SparkSession
    """
    return (
        SparkSession.builder.config(conf=spark_conf)
        .appName(app_name)
        .getOrCreate()
    )
        
class DataConfig:
    """Data Path Configuration Class"""
    
    SCHEMELESS_DATA_TRAIN = f"data/1-Schemeless-Raw-Data/train.csv"
    SCHEMELESS_DATA_TEST = f"data/1-Schemeless-Raw-Data/test.csv"
    
    SCHEMATIC_DATA_TRAIN = f"data/2-Schematic-Raw-Data/train.csv"
    SCHEMATIC_DATA_TEST = f"data/2-Schematic-Raw-Data/test.csv"
        
    PREPROCESSED_DATA_TRAIN = f"data/3-Preprocessed-Data/train.csv"
    PREPROCESSED_DATA_TEST = f"data/3-Preprocessed-Data/test.csv"
    
    FEATURE_DATA = f"data/4-Feature-Store/features.csv"
    SCALED_FEATURE_DATA = "data/4-Feature-Store/scaled-features.csv"
    
class SchemaConfig:
    """Schema Configuration Class"""
    
    DATA_RAW_SCHEMA = StructType(
    [
        StructField("PassengerId", StringType(), True),
        StructField("HomePlanet", StringType(), True),
        StructField("CryoSleep", BooleanType(), True),
        StructField("Cabin", StringType(), True),
        StructField("Destination", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("VIP", BooleanType(), True),
        StructField("RoomService", FloatType(), True),
        StructField("FoodCourt", FloatType(), True),
        StructField("ShoppingMall", FloatType(), True),
        StructField("Spa", FloatType(), True),
        StructField("VRDeck", FloatType(), True),
        StructField("Name", StringType(), True),
        StructField("Transported", BooleanType(), True),
    ]
    )
    
    DATA_FEATURIZED_SCHEMA = StructType(
    [
        StructField("HomePlanet", StringType(), True),
        StructField("CryoSleep", BooleanType(), True),
        StructField("Destination", StringType(), True),
        StructField("Age", IntegerType(), True),
        StructField("VIP", BooleanType(), True),
        StructField("RoomService", FloatType(), True),
        StructField("FoodCourt", FloatType(), True),
        StructField("ShoppingMall", FloatType(), True),
        StructField("Spa", FloatType(), True),
        StructField("VRDeck", FloatType(), True),
        StructField("Transported", BooleanType(), True),
        StructField("CabinDeck", StringType(), True),
        StructField("CabinSide", StringType(), True),
    ]
    )
    

class ColumnConfig:
    """Column Configuration Class"""
      
    CATEGORICAL_COLUMNS = ['PassengerId','HomePlanet','Cabin','Destination','Name']
    NUMERICAL_COLUMNS = ['Age','RoomService','FoodCourt','ShoppingMall','Spa','VRDeck']
    BOOLEAN_COLUMNS = ["CryoSleep", "VIP"]
    
    CATEGORICAL_COLUMNS_FEATURIZED = ['HomePlanet','Destination','CabinDeck','CabinSide']
    NUMERICAL_COLUMNS_FEATURIZED = ['Age','RoomService','FoodCourt','ShoppingMall','Spa','VRDeck']
    
    TARGET_COLUMN = "Transported"