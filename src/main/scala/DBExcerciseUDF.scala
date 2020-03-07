import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{count, explode, substring, row_number}

object DBExcerciseUDF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("basics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataDir = "/Users/harumughan/IdeaProjects/spark_examples/data/"
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.option("inferSchema", "true").option("header", "true").option("delimiter", "|").csv(dataDir + "test.dat")
    df.show()

//    Below we've created a small DataFrame. You should use DataFrame API functions and UDFs to accomplish two tasks.
//
//    You need to parse the State and city into two different columns.
//    You need to get the number of days in between the start and end dates. You need to do this two ways.
//      Firstly, you should use SparkSQL functions to get this date difference.
//      Secondly, you should write a udf that gets the number of days between the end date and the start date.

    df.withColumn("city")


  }

}
