object LimitCollect extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val input = spark.range(50).withColumn("key", $"id" % 5)
  val dataFrame = input.groupBy("key").agg(collect_set("id").as("all"))
  val solution = dataFrame.withColumn("three", filter($"all", (elem, i) => i < 3))
//  read about slice on dataframe
  solution.show(truncate = false)
}
