Can you solve this simple PySpark Transformation Challenge? 📣 

𝐈𝐧𝐟𝐨:

You have a dataset that logs attendance for employees on different dates. 
The goal is to determine the longest streak of consecutive days attended by each employee and indicate whether the streak is still active.



# Solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, datediff, row_number, max, count, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Attendance Streak").getOrCreate()

# Sample data
data = [
    (1, "2024-11-01"),
    (1, "2024-11-02"),
    (1, "2024-11-05"),
    (2, "2024-11-03"),
    (2, "2024-11-04"),
    (2, "2024-11-05")
]

# Create DataFrame
df = spark.createDataFrame(data, ["employee_id", "attendance_date"])

# Convert attendance_date to DateType
df = df.withColumn("attendance_date", col("attendance_date").cast("date"))

# Define a window partitioned by employee_id and ordered by attendance_date
window_spec = Window.partitionBy("employee_id").orderBy("attendance_date")

# Calculate the difference in days from the previous row
df = df.withColumn("prev_date", lag("attendance_date").over(window_spec))
df = df.withColumn("date_diff", datediff("attendance_date", "prev_date"))

# Identify the start of a new streak (where date_diff is greater than 1 or null)
df = df.withColumn("streak_id", 
                   count(when(col("date_diff").isNull() | (col("date_diff") > 1), 1))
                   .over(window_spec))

# Calculate the length of each streak by counting rows within each streak_id
streak_window = Window.partitionBy("employee_id", "streak_id")
df = df.withColumn("streak_length", count("attendance_date").over(streak_window))

# Get the longest streak length for each employee
max_streak_window = Window.partitionBy("employee_id")
df = df.withColumn("max_streak", max("streak_length").over(max_streak_window))

# Determine if the longest streak is active (i.e., if the latest attendance is part of this streak)
df = df.withColumn("is_active_streak", 
                   when((col("streak_length") == col("max_streak")) & 
                        (row_number().over(window_spec.orderBy(col("attendance_date").desc())) == 1), True)
                   .otherwise(False))

# Select final results for each employee
result = df.filter(col("streak_length") == col("max_streak")) \
           .groupBy("employee_id") \
           .agg(max("streak_length").alias("longest_streak"),
                max("is_active_streak").alias("is_active_streak"))

# Show the result
result.show()
