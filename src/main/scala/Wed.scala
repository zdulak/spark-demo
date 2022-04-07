object Wed extends App {
  val path = if (args.length > 0) args(0) else "data.csv"

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("Wed")
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  val data = spark.read.option("header", true).option("inferSchema", true).csv(path)
  val dataUpper = data.columns.foldLeft(data)((frame, name) => frame.withColumn(name, upper(col(name))))
  dataUpper.show()



}
