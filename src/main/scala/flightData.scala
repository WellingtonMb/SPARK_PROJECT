import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._

object flightData {

  import org.apache.spark.sql.{DataFrame, SparkSession}

    private val spark: SparkSession = SparkSession.builder()
      .master("local")
      //.config("spark.master", "local")
      .appName("flight data app")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val flightsDF: DataFrame = spark.read
    .format("csv")
    .option("nullValue", "NA")
    .option("inferSchema", "true")
    //.option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
    .option("mode", "failfast")
    .option("header", "true")
    .load("C:/SparkSession/Datasets/flightSummary.csv")

  flightsDF.printSchema()

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  val sortedflightsDF: Dataset[Row] = flightsDF.sort("count")

  //val flightsDF1: DataFrame = flightsDF.selectExpr("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME","count")
  flightsDF.createOrReplaceTempView("flightsTV")

  val sqlWay: DataFrame = spark.sql(
    """SELECT DEST_COUNTRY_NAME, count(1)
      |FROM flightsTV
      |GROUP BY DEST_COUNTRY_NAME""".stripMargin)

  val sortedGrouped: DataFrame = spark.sql(
    """SELECT DEST_COUNTRY_NAME, sum(count) as destination_count
      |FROM flightsTV
      |GROUP BY DEST_COUNTRY_NAME
      |ORDER BY sum(count) DESC
      |LIMIT 5""".stripMargin)

  sortedGrouped.write
      .format("csv")
                .mode("overwrite")
                .save("src/main/Resources")

  val scalaWay1: Dataset[Row] = flightsDF.groupBy("DEST_COUNTRY_NAME")
                          .sum("count")
                          .withColumnRenamed("sum(count)", "Total")
                          .sort(desc("Total"))
                          .limit(5)


  def main(args: Array[String]): Unit={

    flightsDF.show(5)
    flightsDF.sort("count").explain()
    sortedflightsDF.show()
    sqlWay.show()
    sortedGrouped.show(5)
    scalaWay1.show()

  }
}
