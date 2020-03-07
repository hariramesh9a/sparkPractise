import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

object spark_vegas {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello World").master("local[*]").getOrCreate()
    val logData = spark.read.option("header", true).option("inferSchema", true).csv("/Users/harumughan/PycharmProjects/spark_etl/data/wine_price.csv")
    logData.show()

    logData.printSchema()


    val plot = Vegas("approval date").
      withDataFrame(logData).
      mark(Bar).
      encodeX("price", Quant, bin = Bin(maxbins = 20.0), sortOrder = SortOrder.Desc).
      show




  }

}
