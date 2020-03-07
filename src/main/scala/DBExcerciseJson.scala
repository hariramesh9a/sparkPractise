import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import java.net.URL
import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, explode, substring, row_number}


object DBExcerciseJson {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("basics").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataDir = "/Users/harumughan/IdeaProjects/spark_examples/data/"
    val tmpFile = new File(dataDir + "rows.json")
    FileUtils.copyURLToFile(new URL("https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json?accessType=DOWNLOAD"), tmpFile)
    val sqlcontext = new SQLContext(sc)
    val babyDF = sqlcontext.read.option("multiLine", "true").json(dataDir + "rows.json")
    babyDF.printSchema()

    babyDF.createOrReplaceTempView("babyView")
    //    sqlcontext.sql("select meta.view.averageRating from babyView").show(10
    //    )

    import sqlcontext.implicits._

    val df = babyDF.select(explode($"data").as("rows"))

    val explodedDf = df.select(
      (0 until 13).map(i => df("rows")(i).alias(s"cols$i")): _*
    )

    val nameDF = explodedDf.select($"cols8".as("year"), substring($"cols9", 1, 1).as("first_letter"))

    val aggDF = nameDF.groupBy($"year", $"first_letter").agg(count('first_letter).as("letter_count"))

    val w = Window.partitionBy($"year").orderBy($"letter_count".desc)

    val dfTopLetters = aggDF.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")

    dfTopLetters.show()


  }

}
