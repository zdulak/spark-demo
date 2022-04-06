object SparkDemo extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.text("build.sbt")
  df.show(truncate = false)
  df.write.text("output")
//  spark.read.text("output").show(truncate = false)
}
