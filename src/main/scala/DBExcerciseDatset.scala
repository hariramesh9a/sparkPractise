import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{count, explode, substring, row_number}


object DBExcerciseDatset {

  case class LogClass(ip: String, col1: String, col2: String, date: String, ts: String, get: String, code: Int, bytes: Int, url: String, agent: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("basics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataDir = "/Users/harumughan/IdeaProjects/spark_examples/data/"

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val schema = StructType(Array(
      StructField("ip", StringType, true),
      StructField("col1", StringType, true),
      StructField("col2", StringType, true),
      StructField("date", StringType, true),
      StructField("ts", StringType, true),
      StructField("get", StringType, true),
      StructField("code", IntegerType, true),
      StructField("bytes", IntegerType, true),
      StructField("url", StringType, true),
      StructField("agent", StringType, true))
    )


    val df = sqlContext.read.option("delimiter", " ").schema(schema).option("header", "false").option("inferSchema", "true").format("csv").load(dataDir + "logs.dat")
    df.show()


    val df4 = df.as[LogClass]
    df4.show()

    df.groupBy('ip).agg(count($"ip")).show()


  }

}
