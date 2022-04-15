object SplitWithVariableDelimiter extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("SplitWithVariableDelimiter")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val dept = Seq(
    ("50000.0#0#0#", "#"),
    ("0@1000.0@", "@"),
    ("1$", "$"),
    ("1000.00^Test_string", "^")).toDF("value", "delimiter")

  val solution = dept
    .as[Table]
//    .take(dept.count.toInt)
    .map {
      case Table(value, delimiter) => (value, delimiter, value.split(delimiter(0)))
    }
//    .toSeq
    .toDF("value", "delimiter", "split_values")
  solution.show(truncate = false)

  import org.apache.spark.sql.functions._

  val splitUdf = udf { (str: String, delimiter: String) => str.split("\\" + delimiter) }
  val solution2 = dept.withColumn("split_values", splitUdf(col("value"), col("delimiter")))
  solution2.show(truncate = false)

  val solution3 = dept
    .map(row => (row.getString(0), row.getString(1), row.getString(0).split("\\" + row.getString(1))))
    .toDF("value", "delimiter", "split_values")
  solution3.show(truncate = false)
}

case class Table(value: String, delimiter: String)
