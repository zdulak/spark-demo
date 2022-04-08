object FlatMapNumbers extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("FlatMapNumbers")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val nums = Seq(Seq(1, 2, 3), Seq(4, 5, 6)).toDF("nums")

  val solution = nums.flatMap(row => {
    val numbers = row.getSeq[Int](0)
    numbers.map(elem => (numbers, elem))
  }).toDF("nums", "num")

  solution.show()

  val solution2 = nums.withColumn("num", explode($"nums"))
  solution2.show()
}
