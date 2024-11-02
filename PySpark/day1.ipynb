'''𝐆𝐚𝐦𝐞 𝐏𝐥𝐚𝐲 𝐀𝐧𝐚𝐥𝐲𝐬𝐢𝐬 𝐈𝐈
Write a pyspark code that reports the device that is first logged in for each player.
Return the result table in any order.

expected output : Player_id, device_id
Difficult Level : EASY
#-----------Data and Schema----------
data = [
 (1, 2, '2016-03-01', 5),
 (1, 2, '2016-05-02', 6),
 (2, 3, '2017-06-25', 1),
 (3, 1, '2016-03-02', 0),
 (3, 4, '2018-07-03', 5)
]

  schema =player_id,device_id,event_date,games_played
#----------PySpark Code---------------
    '''
    import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DAY1").getOrCreate()

#Define Data

data = [
 (1, 2, '2016-03-01', 5),
 (1, 2, '2016-05-02', 6),
 (2, 3, '2017-06-25', 1),
 (2, 3, '2015-06-25', 1),
 (3, 1, '2016-03-02', 0),
 (3, 4, '2018-07-03', 5)
]
# define schema
schema =["player_id","device_id","event_date",",games_played"]
    #define Data frame
df=spark.createDataFrame(data,schema)
    
dac = df.groupBy("player_id").agg(min("event_date").alias("mindate"))
dac_df = dac.select(col("player_id").alias("pl_id"),"mindate")
joined_data= dac_df.join(df,(df.player_id == dac_df.pl_id)&(df.event_date == dac_df.mindate) ,"inner")
joined_data.select("player_id","device_id").show()
