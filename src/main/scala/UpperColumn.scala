object UpperColumn extends App {
  if (args.length < 2) {
    import scala.sys.exit
    println("You must enter at least two arguments")
    exit(-1)
  }

  val (path, columnsToUpper) = (args.head, args.tail)

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("FlatMapNumbers")
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._

  val data = spark.read.option("header", true).option("inferSchema", true).csv(path)
  val solution = columnsToUpper.foldLeft(data)((frame, name) => {
    frame.withColumn("upper_" + name, upper(col(name)))
  })
  solution.show()
}
