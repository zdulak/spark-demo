object FindMostCommonPrefix2 extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("FlatMapNumbers")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val input = Seq(
    (1, "Mr"),
    (1, "Mza"),
    (1, "Mr"),
    (1, null),
    (1, null),
    (1, null),
    (2, "Mr"),
    (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")

  val groupedInput  = input.groupBy($"UNIQUE_GUEST_ID").pivot($"PREFIX").count()
    .withColumn("null", lit(-1))
  val columnsWithNames = groupedInput.columns.tail
    .map(c => struct(col(c).as("value"), lit(c).as("name")))
  val solution = groupedInput
    .withColumn("MAX_PREFIX", greatest(columnsWithNames: _*).getItem("name"))
    .drop(groupedInput.columns.tail: _*)
  solution.show()
}
