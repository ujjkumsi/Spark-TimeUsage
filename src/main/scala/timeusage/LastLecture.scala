package timeusage

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * Created by Ujjwal on 25/11/17.
  */

object LastLecture {

  val sparkSession: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()

  import sparkSession.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    //convert simple list to ds
    val sampleDS = List((3, "me"), (2, "you"), (1, "kappa"), (2, "awe"), (1, "me"), (3, "lol"), (3, "pappu"), (2, "me"), (2, "trip")).toDS()

    //replicating reducebykey behaviour => rdd.reduceByKey(_+_)
    sampleDS.groupByKey(p => p._1)
      .mapValues(p => p._2)
      .reduceGroups((acc, p) => acc + p)
      .sort($"value")
      .show()
    //using aggregator
    val strConcat = new Aggregator[(Int, String), String, String] {
      def zero: String = ""

      def reduce(a: String, b: (Int, String)): String = a + b._2

      def merge(b1: String, b2: String): String = b1 + b2

      def finish(r: String): String = r

      def bufferEncoder: Encoder[String] = Encoders.STRING

      def outputEncoder: Encoder[String] = Encoders.STRING
    }.toColumn

    sampleDS.groupByKey(pair => pair._1)
      .agg(strConcat.as[String])
      .show()
  }
}
