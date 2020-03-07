import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DBExcercise1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("basics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val dataDir = "/Users/harumughan/IdeaProjects/spark_examples/data/"
    val supplierRDD = sc.textFile(dataDir + "supplier.dat").map(x => x.split('|')).map(fields => (fields(3).toInt, (fields(0), fields(1))))
    val nationRDD = sc.textFile(dataDir + "nation.dat").map(x => x.split('|')).map(fields => (fields(0).toInt, (fields(0), fields(1), fields(2))))
    supplierRDD.foreach(x => println(x))
    supplierRDD.leftOuterJoin(nationRDD).foreach(println(_))
    val sqlcontext = new SQLContext(sc)

    import sqlcontext.implicits._


    val supDf = sqlcontext.read.option("delimiter", "|").csv(dataDir + "supplier.dat").toDF("supKey", "name", "address", "nationKey", "phone", "acctBal", "comment")
    val nationDf = sqlcontext.read.option("delimiter", "|").csv(dataDir + "nation.dat").toDF("nationKey", "nationName", "regionKey", "comment")
    val joinedDf=supDf.join(nationDf,Seq("nationKey"),"leftOuter")
    joinedDf.show()




  }

}
