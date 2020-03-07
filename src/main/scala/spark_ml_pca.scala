import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{ StringIndexerModel, VectorAssembler }

object spark_ml_pca {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello World").master("local[*]").getOrCreate()
    val logData = spark.read.option("header",true).option("inferSchema",true).csv("/Users/harumughan/PycharmProjects/spark_etl/data/csv_sample.csv")
    logData.show()

    logData.printSchema()

    val cols = logData.drop("market_date_hour_unix_time","market_date_hours")
    val cols_cleansed=cols.na.fill(0)

    val assembler = new VectorAssembler().setInputCols(cols_cleansed.columns).setOutputCol("features")
    val output_dat = assembler.transform(logData).select("market_date_hours")
    output_dat.show(10)


  }

}
