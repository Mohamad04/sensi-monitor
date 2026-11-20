import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Main {
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

    volumes.show(30, truncate = false)

    spark.stop()
    spark.stop()
  }
}