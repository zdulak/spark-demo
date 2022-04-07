object FlatMapNumbers extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("FlatMapNumbers")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val nums = Seq(Seq(1, 2, 3)).toDF("nums")

  val solution = nums.flatMap(row => {
    val numbers = row.getSeq[Int](0)
    numbers.map(elem => (numbers, elem))
  }).toDF("nums", "num")

  solution.show()
}
