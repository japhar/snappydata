package snappydata.benchmark

/**
  * Created by japhar on 09/03/18.
  * Spark Submit Command:
  * spark-submit --class snappydata.benchmark.SparkCassandraPerf --master local[2] --jars /usr/local/Cellar/apache-spark/2.3.0/libexec/jars/spark-hive_2.11-2.3.0.jar --conf spark.cassandra.connection.host=127.0.0.1 --packages "datastax:spark-cassandra-connector:2.0.7-s_2.11" /Users/japhar/apache/spark/snappydata/target/spark.snappydata-1.0-SNAPSHOT.jar

  */

import java.io.{File, FileOutputStream, PrintStream, PrintWriter}

import com.datastax.spark.connector.DataFrameFunctions
import scala.util.{Failure, Success, Try}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.slick.lifted.Query


object SparkCassandraPerf extends Serializable {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder.appName("SparkCassandraPerfTesting")
      .config("spark.cassandra.connection.host", SPARK_CONNECTION_HOST)
      .config("spark.cassandra.auth.username", SPARK_CASSANDRA_AUTH_USERNAME)
      .config("spark.cassandra.auth.password", SPARK_CASSANDRA_AUTH_PASSWORD)
      .config("spark.master", SPARK_MASTER)
      .getOrCreate

    val sqlContext = sparkSession.sqlContext

    CassandraConnector(sparkSession.sparkContext.getConf).withSessionDo { session =>
      session.execute("DROP KEYSPACE IF EXISTS snappy_perf_test")
      session.execute("CREATE KEYSPACE snappy_perf_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    }

    val airlineRefCassandraOptions =
      Map("table"    -> SPARK_CASSANDRA_TABLE,
          "keyspace" -> SPARK_CASSANDRA_KEYSPACE,
          "spark.cassandra.read.timeout_ms" -> "10000")

    val pw = new PrintWriter(SPARK_CASSANDRA_RESULT_FILE)
    Try {

      val airlineCassandraOptions = Map("table" -> SPARK_CASSANDRA_TABLE,
        "keyspace" -> SPARK_CASSANDRA_KEYSPACE,
        "spark.cassandra.input.fetch.size_in_rows" -> "200000",
        "spark.cassandra.read.timeout_ms" -> "10000")
      val airlineDataFrame = SparkSnappyDataPerf.getAirlineDataFrame(sqlContext)
      var airlineFrameFunctions: DataFrameFunctions = new DataFrameFunctions(airlineDataFrame)
      airlineFrameFunctions.createCassandraTable(
        "snappy_perf_test",
        "airline",
        partitionKeyColumns = Some(Seq("UniqueCarrier")),
        clusteringKeyColumns = Some(Seq("id")))

      airlineDataFrame.printSchema()

      println("loading data")
      var start = System.currentTimeMillis
      airlineDataFrame.write.format("org.apache.spark.sql.cassandra").options(airlineCassandraOptions).save()

      var end = System.currentTimeMillis
      pw.println(s"\nTime to load into table airline = " +
        (end - start) + " ms")

      println("loading completed")

      var airlinedataFrameCassandra = sqlContext.read.format("org.apache.spark.sql.cassandra")
        .options(airlineRefCassandraOptions)
        .load()

      airlinedataFrameCassandra.createOrReplaceTempView("airlineSpark")

      val queries = List(SPARK_QUERY1,SPARK_QUERY2,SPARK_QUERY3,SPARK_QUERY4)
      queries.foreach(query => {
        pw.println(s"\nQuery: ${query}")
        sqlContext.sql(query).explain()
        val startTime = System.currentTimeMillis
        val data = sqlContext.sql(query).collect()
        var endTime = System.currentTimeMillis
        pw.println(s"Time Taken to execute query = " +
          (endTime - startTime) + " ms")
        data.foreach(println(_))
      })

    } match {
      case Success(v) => pw.close()
      case Failure(e) =>
        e.printStackTrace(pw)
        pw.println("Exception occurred while executing the job " +
          "\nError Message:" + e.getMessage)
        pw.close()
    }
    }
}
