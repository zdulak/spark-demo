//The FindMostCommonPrefix and FindMostCommonPrefix2 objects contain two different solutions of the exercise: <https://jaceklaskowski.github.io/spark-workshop/exercises/spark-sql-exercise-Finding-Most-Common-Non-null-Prefix-Occurences-per-Group.html>

object FindMostCommonPrefix extends App {
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
    (1, "Mme"),
    (1, "Mr"),
    (1, null),
    (1, null),
    (1, null),
    (2, "Mr"),
    (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")

  val groupedInput = input.groupBy($"UNIQUE_GUEST_ID").agg(collect_list($"PREFIX"))
  val solution = groupedInput.as[(Int, Seq[String])]
    .map { case (id, prefixes) => (id, maxPrefix(prefixes))}
    .toDF("UNIQUE_GUEST_ID", "Max_prefix")
  solution.show()

  def maxPrefix(prefixes: Seq[String]): String = {
    if (prefixes.isEmpty) null else {
      val prefixesOcc = prefixes.groupBy(identity).map { case (prefix, prefixList) => (prefix, prefixList.length)}
      prefixesOcc.maxBy { case (_, occ) => occ}._1
    }
  }

}
