import java.io.File
import java.net.URL

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, explode, row_number, substring}
import org.apache.spark.{SparkConf, SparkContext}


object DBExcerciseSeq {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("basics").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc)

    import sqlcontext.implicits._

    val full_csv = sc.parallelize(Array(
      "col_1, col_2, col_3",
      "1, ABC, Foo1",
      "2, ABCD, Foo2",
      "3, ABCDE, Foo3",
      "4, ABCDEF, Foo4",
      "5, DEF, Foo5",
      "6, DEFGHI, Foo6",
      "7, GHI, Foo7",
      "8, GHIJKL, Foo8",
      "9, JKLMNO, Foo9",
      "10, MNO, Foo10"))

    val removeHead = full_csv.mapPartitionsWithIndex(
      (i, loop) =>
        if (i == 0 && loop.hasNext) {
          loop.next
          loop
        } else loop)

    val head = full_csv.first().split(",")
    val df = removeHead.map(x => x.split(",")).map(c => (c(0), c(1), c(2))).toDF()
    //    df.select(head.zipWithIndex.map { case (a, b) => df("_" + (b+1).toString).alias(a) }: _*).show()

    val df1 = df.toDF(head: _*)
    df1.show()


    val csv = sc.parallelize(Seq(Seq("Row-Key-001", "K1", "10", "A2", "20", "K3", "30", "B4", "42", "K5", "19", "C20", "20"), Seq(
      "Row-Key-002", "X1", "20", "Y6", "10", "Z15", "35", "X16", "42")
    ))
    val df2=csv.map(x=>(x.head,x.tail)).toDF("key","elements").select($"key",explode($"elements"))




  }

}
