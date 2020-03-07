
import org.apache.spark.sql.SparkSession


object helloworld {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Hello World").master("local[*]").getOrCreate()
    val logfile = s"test.dat"
    val logData = spark.read.textFile(logfile)
    val num = logData.filter(line => line.contains("Hari")).count()
    println(s"Count is $num")

  }

}
