import java.io.File
import java.net.URL

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DBExcerciseAPI {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("basics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)


    val url = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json"
    val jsonResult = List(scala.io.Source.fromURL(url).mkString)
    val jsonRDD=sc.makeRDD(jsonResult)
    println(jsonRDD.count())
    val jsonDF=sqlcontext.read.json(jsonRDD)
    jsonDF.printSchema()

  }

}
