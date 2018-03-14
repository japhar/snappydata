package snappydata.benchmark

/**
  * Created by japhar on 09/03/18.
  * Spark Submit Command:
  * spark-submit  --class snappydata.benchmark.SparkSnappyDataPerf --master local[*] --conf spark.snappydata.connection=localhost:1527 --packages "SnappyDataInc:snappydata:1.0.1-s_2.11" /Users/japhar/apache/spark/snappydata/target/spark.snappydata-1.0-SNAPSHOT.jar
  */

import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession,SnappySession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.DriverManager
import scala.util.Try

object SparkSnappyDataPerf extends Serializable{

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder
      .appName("SparkSnappySmartConnectorPerfTesting")
      .config("spark.master", SPARK_MASTER)
      .config("spark.snappydata.connection", SPARK_SNAPPYDATA_CONNECTION)
      .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
    val pw = new PrintWriter(SPARK_SNPPY_RESULT_FILE)

    Try {

      val airlineDataFrame = getAirlineDataFrame(snSession.sqlContext)
      snSession.dropTable("snappy_perf_test.airline", ifExists = true)
      val p1 = Map("PERSISTENCE" -> "NONE")
      snSession.createTable("snappy_perf_test.airline", "column", airlineDataFrame.schema, p1)
      var start = System.currentTimeMillis
      airlineDataFrame.write.insertInto("snappy_perf_test.airline")
      var end = System.currentTimeMillis
      pw.println(s"\nTime taken to load data into airline table = " + (end - start) + " ms")

      //Queries1 & 2 - Aggregation queries
      //Queries3 & 4 - Point based queries
      val queries = List(SPARK_QUERY1,SPARK_QUERY2,SPARK_QUERY3,SPARK_QUERY4)

      queries.foreach(query => {
        pw.println(s"\nQuery: ${query}")
        snSession.sqlContext.sql(query).explain()
        val startTime = System.currentTimeMillis
        val data = snSession.sqlContext.sql(query).collect()
        var endTime = System.currentTimeMillis
        pw.println(s"Time Taken to execute query = " +
          (endTime - startTime) + " ms")
        data.foreach(println(_))
      })

    } match {
      case Success(v) => pw.close()
      case Failure(e) =>
        pw.println("Exception occurred while executing the job " + "\nError Message:" + e
          .getMessage)
        e.printStackTrace(pw)
        pw.close()
    }
  }

  def getAirlineDataFrame(sqlContext: SQLContext): DataFrame = {
    val tempAirlineDataFrame: DataFrame =
      sqlContext
        .read
        .format(SPARK_INPUTFORMAT)
        .load(SPARK_SRC_DIR)

    tempAirlineDataFrame.createOrReplaceTempView("tempAirline")
    val airlineDataFrame = sqlContext.sql("select monotonically_increasing_id() as id, * from tempAirline")
    println(airlineDataFrame.schema.mkString(","))
    airlineDataFrame.na.fill(0L)
    airlineDataFrame
  }

}
