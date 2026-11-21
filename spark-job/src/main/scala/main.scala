import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Main {
  private val LowVolumeRatio = 0.7

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sensi-monitor")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val raw = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "trades.raw")
      .option("startingOffsets", "earliest")
      .load()



    // 1. The shape of our JSON
    val tradeSchema = StructType(Seq(
      StructField("tradeId", StringType),
      StructField("source", StringType),
      StructField("desk", StringType),
      StructField("currency", StringType),
      StructField("delta", DoubleType),
      StructField("vega", DoubleType),
      StructField("asOfDate", StringType)
    ))

    // 2. Integration + normalization
    val trades = raw
      .select(from_json(col("value").cast("string"), tradeSchema).as("t"))
      .select("t.*")
      .withColumn("source", upper(trim(col("source"))))
      .withColumn("asOfDate", to_date(col("asOfDate")))

    // 3. Daily aggregation per source
    val volumes = trades
      .groupBy("asOfDate", "source")
      .agg(
        count("*").as("trades"),
        round(sum("delta"), 2).as("totalDelta")
      )
      .orderBy("asOfDate", "source")

    // 1. The most recent date in the data
    val today = volumes.agg(max("asOfDate")).first().getDate(0)

    // 2. The average of the previous days, per source
    val history = volumes
      .filter(col("asOfDate") < lit(today))
      .groupBy("source")
      .agg(round(avg("trades"), 2).as("avgPrevious"))

    // 3. Today's volumes
    val todayVolumes = volumes
      .filter(col("asOfDate") === lit(today))
      .select("source", "trades")

    // 4. Comparison and status
    val alerts = todayVolumes
      .join(history, "source")
      .withColumn("threshold", round(col("avgPrevious") * LowVolumeRatio, 2))
      .withColumn("status",
        when(col("trades") < col("threshold"), "LOW_VOLUME").otherwise("OK"))
      .orderBy("source")


    // Connection to PostgreSQL
    val jdbcUrl = "jdbc:postgresql://localhost:5432/sensimonitor"
    val jdbcProps = new java.util.Properties()
    jdbcProps.put("user", "sensi")
    jdbcProps.put("password", "sensi")
    jdbcProps.put("driver", "org.postgresql.Driver")

    // Daily volumes → table daily_volumes
    volumes
      .select(
        col("asOfDate").as("as_of_date"),
        col("source"),
        col("trades"),
        col("totalDelta").as("total_delta"))
      .write.mode("overwrite")
      .jdbc(jdbcUrl, "daily_volumes", jdbcProps)

    // Today's checks → table volume_alerts
    alerts
      .select(
        lit(today).as("as_of_date"),
        col("source"),
        col("trades"),
        col("avgPrevious").as("avg_previous"),
        col("threshold"),
        col("status"))
      .write.mode("overwrite")
      .jdbc(jdbcUrl, "volume_alerts", jdbcProps)

    println("Results written to PostgreSQL")
    spark.stop()
  }
}