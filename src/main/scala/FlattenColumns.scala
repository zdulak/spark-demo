import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition.first

//The FlattenColumns object contains the solution of the exercise: <https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Flattening-Array-Columns-From-Datasets-of-Arrays-to-Datasets-of-Array-Elements.html>

object FlattenColumns extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("FlatMapNumbers")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val data = Seq(
    Seq("a","b","c"),
    Seq("X","Y","Z")).toDF("value")

  import org.apache.spark.sql.functions._
  val size = data.as[Seq[String]].head.length

  val solution = (0 until size)
    .foldLeft(data)((frame, index) => frame.withColumn(index.toString, col("value")(index)))
    .drop("value")
  solution.show()
}
