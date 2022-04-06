import org.apache.spark.sql.functions._


object Wed extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Wed")
    .master("local[*]")
    .getOrCreate()

  val data = spark.read.option("header", true).option("inferSchema", true).csv("data.csv")
  val dataUpper = data.columns.foldLeft(data)((frame, name) => frame.withColumn(name, upper(col(name))))
  dataUpper.show()

}
