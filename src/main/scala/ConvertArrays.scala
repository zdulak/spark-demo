//The ConvertArrays object contains the solution of the exercise: <https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Converting-Arrays-of-Strings-to-String.html>

object ConvertArrays extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val words = Seq(Array("hello", "world")).toDF("words")
  val solution  = words.withColumn("solution", concat_ws(" ", $"words"))
  solution.show()

}
