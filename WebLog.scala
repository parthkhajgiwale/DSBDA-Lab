import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.functions.{regexp_extract, sum, col, to_date, udf, to_timestamp, desc, dayofyear, year}

val spark = SparkSession.builder().appName("WebLog").master("local[*]").getOrCreate()
val base_df = spark.read.text("/home/parthk2113/Desktop/weblog.csv")

import spark.implicits._

import spark.implicits._

val parsed_df = base_df.select(
    regexp_extract($"value", """^([^(\s|,)]+)""", 1).alias("host"),
    regexp_extract($"value", """^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""", 1).as("timestamp"),
    regexp_extract($"value", """^.*\w+\s+([^\s]+)\s+HTTP.*""", 1).as("path"),
    regexp_extract($"value", """^.*,([^\s]+)$""", 1).cast("int").alias("status")
)

println("Number of bad row in the initial dataset: " + base_df.filter($"value".isNull).count())

val bad_rows_df = parsed_df.filter($"host".isNull || $"timestamp".isNull || $"path".isNull || $"status".isNull)
println("Number of bad rows: " + bad_rows_df.count())

val cleaned_df = parsed_df.na.drop()

println("The count of null value: " + cleaned_df.filter($"host".isNull || $"timestamp".isNull || $"path".isNull || $"status".isNull).count())
println("Before: " + parsed_df.count() + " | After: " + cleaned_df.count())

val logs_df = cleaned_df.withColumn("time", to_timestamp($"timestamp", "dd/MMM/yyyy:HH:mm:ss")).drop("timestamp").cache()

logs_df.printSchema()

logs_df.show(2)

logs_df.describe("status").show()

logs_df.groupBy("host").count().sort(desc("count")).show(10)

logs_df.groupBy("path").count().sort(desc("count")).show(5)

val unique_host_count = logs_df.select("host").distinct().count()
println("Number of unique Hosts: %d".format(unique_host_count))

val not_found_count = logs_df.filter($"status" === 404).count()
println("Count of 404 Response Codes: %d".format(not_found_count))

logs_df.filter($"status" === 404).groupBy("host").count().sort(desc("count")).show(25, truncate = false)

val unique_daily_hosts_count = logs_df.select("host", "day", "year").distinct().groupBy("day", "year").count().sort("year", "day").count()
println("Number of Unique Daily Hosts: %d".format(unique_daily_hosts_count))
