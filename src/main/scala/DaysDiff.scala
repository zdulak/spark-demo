//The DaysDiff object contains the solution of the exercise: <https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Difference-in-Days-Between-Dates-As-Strings.html>

object DaysDiff extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("FlatMapNumbers")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val dates = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")

  val solution =  dates
    .withColumn("to_date", current_date())
    .withColumn("diff", datediff($"to_date", to_date($"date_string", "d/M/y")))
  solution.show()

}
