import org.apache.spark.sql.functions._


object Wed extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Wed")
    .master("local[*]")
    .getOrCreate()

  val dataFrame = spark.read.option("header", true).option("inferSchema", true).csv("data.csv")
  val newFrame = dataFrame.columns.foldLeft(dataFrame)((frame, name) => frame.withColumn(name, upper(col(name))))
  newFrame.show()

}
