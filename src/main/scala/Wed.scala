import org.apache.spark.sql.functions._


object Wed  extends App {
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  val dataFrame = spark.read.csv("data.csv")
  val newFrame = dataFrame.columns.foldLeft(dataFrame)((frame, name) => frame.withColumn(name, upper(col(name))))
  newFrame.show()

}
