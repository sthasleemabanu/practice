# 𝐖𝐞𝐚𝐭𝐡𝐞𝐫 𝐓𝐲𝐩𝐞 𝐈𝐧 𝐄𝐚𝐜𝐡 𝐂𝐨𝐮𝐧𝐭𝐫𝐲 𝐏𝐫𝐨𝐛𝐥𝐞𝐦
𝐖𝐫𝐢𝐭𝐞 𝐚𝐧 𝐏𝐲𝐬𝐩𝐚𝐫𝐤 𝐜𝐨𝐝𝐞 𝐭𝐨 𝐟𝐢𝐧𝐝 𝐭𝐡𝐞 𝐭𝐲𝐩𝐞 𝐨𝐟 𝐰𝐞𝐚𝐭𝐡𝐞𝐫 𝐢𝐧 𝐞𝐚𝐜𝐡 𝐜𝐨𝐮𝐧𝐭𝐫𝐲 𝐟𝐨𝐫 𝐍𝐨𝐯𝐞𝐦𝐛𝐞𝐫 2019.
𝐓𝐡𝐞 𝐭𝐲𝐩𝐞 𝐨𝐟 𝐰𝐞𝐚𝐭𝐡𝐞𝐫 𝐢𝐬 𝐂𝐨𝐥𝐝 𝐢𝐟 𝐭𝐡𝐞 𝐚𝐯𝐞𝐫𝐚𝐠𝐞 𝐰𝐞𝐚𝐭𝐡𝐞𝐫_𝐬𝐭𝐚𝐭𝐞 𝐢𝐬 𝐥𝐞𝐬𝐬 𝐭𝐡𝐚𝐧 𝐨𝐫 𝐞𝐪𝐮𝐚𝐥 15,
𝐇𝐨𝐭 𝐢𝐟 𝐭𝐡𝐞 𝐚𝐯𝐞𝐫𝐚𝐠𝐞 𝐰𝐞𝐚𝐭𝐡𝐞𝐫_𝐬𝐭𝐚𝐭𝐞 𝐢𝐬 𝐠𝐫𝐞𝐚𝐭𝐞𝐫 𝐭𝐡𝐚𝐧 𝐨𝐫 𝐞𝐪𝐮𝐚𝐥 25 𝐚𝐧𝐝 𝐖𝐚𝐫𝐦 𝐨𝐭𝐡𝐞𝐫𝐰𝐢𝐬𝐞.
 
𝐑𝐞𝐭𝐮𝐫𝐧 𝐫𝐞𝐬𝐮𝐥𝐭 𝐭𝐚𝐛𝐥𝐞 𝐢𝐧 𝐚𝐧𝐲 𝐨𝐫𝐝𝐞𝐫.

  𝐒𝐜𝐡𝐞𝐦𝐚 𝐀𝐧𝐝 𝐃𝐚𝐭𝐚:
================
 # Define the schema for the Weather DataFrame
weather_schema = StructType([
 StructField("country_id", IntegerType(), True),
 StructField("weather_state", IntegerType(), True),
 StructField("day", StringType(), True)
])
 
# Weather data
weather_data = [
 (2, 15, "2019-11-01"),
 (2, 12, "2019-10-28"),
 (2, 12, "2019-10-27"),
 (3, -2, "2019-11-10"),
 (3, 0, "2019-11-11"),
 (3, 3, "2019-11-12"),
 (5, 16, "2019-11-07"),
 (5, 18, "2019-11-09"),
 (5, 21, "2019-11-23"),
 (7, 25, "2019-11-28"),
 (7, 22, "2019-12-01"),
 (7, 20, "2019-12-02"),
 (8, 25, "2019-11-05"),
 (8, 27, "2019-11-15"),
 (8, 31, "2019-11-25"),
 (9, 7, "2019-10-23"),
 (9, 3, "2019-12-23")
]
# Define the schema for the Countries DataFrame
countries_schema = StructType([
 StructField("country_id", IntegerType(), True),
 StructField("country_name", StringType(), True)
])
 
countries_data = [
 (2, "USA"),
 (3, "Australia"),
 (7, "Peru"),
 (5, "China"),
 (8, "Morocco"),
 (9, "Spain")
]

#  Source code:

  
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark =SparkSession.builder.appName('weatherdataAnalysis').getOrCreate()

weather_schema = StructType([
 StructField("country_id", IntegerType(), True),
 StructField("weather_state", IntegerType(), True),
 StructField("day", StringType(), True)
])
 
# Weather data
weather_data = [
 (2, 15, "2019-11-01"),
 (2, 12, "2019-10-28"),
 (2, 12, "2019-10-27"),
 (3, -2, "2019-11-10"),
 (3, 0, "2019-11-11"),
 (3, 3, "2019-11-12"),
 (5, 16, "2019-11-07"),
 (5, 18, "2019-11-09"),
 (5, 21, "2019-11-23"),
 (7, 25, "2019-11-28"),
 (7, 22, "2019-12-01"),
 (7, 20, "2019-12-02"),
 (8, 25, "2019-11-05"),
 (8, 27, "2019-11-15"),
 (8, 31, "2019-11-25"),
 (9, 7, "2019-10-23"),
 (9, 3, "2019-12-23")
]
# Define the schema for the Countries DataFrame
countries_schema = StructType([
 StructField("country_id", IntegerType(), True),
 StructField("country_name", StringType(), True)
])
 
countries_data = [
 (2, "USA"),
 (3, "Australia"),
 (7, "Peru"),
 (5, "China"),
 (8, "Morocco"),
 (9, "Spain")
]


weather_df = spark.createDataFrame(data=weather_data,schema=weather_schema)

countries_df = spark.createDataFrame(data=countries_data,schema=countries_schema)

#weather_df.show()
#countries_df.show()

weather_avg_df = weather_df.groupBy("country_id").agg(avg("weather_state").alias("weather_avg"))
weather_avg_df = weather_avg_df.withColumn("weather_type", when(weather_avg_df.weather_avg <=15 , "Cold")
                                        .when(weather_avg_df.weather_avg>=25 , "Hot")
                                        .otherwise("Warm"))
result_df = weather_avg_df.join(countries_df,weather_avg_df.country_id == countries_df.country_id,"left")
result_df.select("country_name","weather_type").show()
  
