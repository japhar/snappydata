package snappydata

import java.util.Properties
import org.apache.spark.SparkException

/**
  * Created by japhar on 24/02/18.
  */
package object benchmark {

  private object SparkBuildInfo {

    val (
      spark_version: String,
      query1: String,
      query2: String,
      query3: String,
      query4: String,
      snappydata_connection: String,
      spark_master: String,
      spark_inputformat: String,
      spark_src_dir: String,
      snappy_results_file: String,
      cassandra_keyspace: String,
      cassandra_table: String,
      cassandra_results_file: String,
      spark_cassandra_connection_host: String,
      spark_cassandra_auth_username: String,
      spark_cassandra_auth_password: String) = {

      val resourceStream = Thread.currentThread().getContextClassLoader.
        getResourceAsStream("perf_benchmark-info.properties")

      if (resourceStream == null) {
        throw new SparkException("Could not find perf_benchmark-info.properties")
      }

      try {
        val unknownProp = "<unknown>"
        val props = new Properties()
        props.load(resourceStream)
        (
          props.getProperty("version", unknownProp),
          props.getProperty("Query1", unknownProp),
          props.getProperty("Query2", unknownProp),
          props.getProperty("Query3", unknownProp),
          props.getProperty("Query4", unknownProp),
          props.getProperty("snappydata.connection", unknownProp),
          props.getProperty("spark.master", unknownProp),
          props.getProperty("spark.inputformat", unknownProp),
          props.getProperty("spark.src.dir", unknownProp),
          props.getProperty("snappy.results.file", unknownProp),
          props.getProperty("cassandra.keyspace", unknownProp),
          props.getProperty("cassandra.table", unknownProp),
          props.getProperty("cassandra.results.file", unknownProp),
          props.getProperty("spark.cassandra.connection.host", unknownProp),
          props.getProperty("spark.cassandra.auth", unknownProp),
          props.getProperty("spark.cassandra.auth", unknownProp)
          )
      } catch {
        case e: Exception =>
          throw new SparkException("Error loading properties from spark-version-info.properties", e)
      } finally {
        if (resourceStream != null) {
          try {
            resourceStream.close()
          } catch {
            case e: Exception =>
              throw new SparkException("Error closing spark build info resource stream", e)
          }
        }
      }
    }
  }
    val SPARK_VERSION = SparkBuildInfo.spark_version
    val SPARK_QUERY1  = SparkBuildInfo.query1
    val SPARK_QUERY2  = SparkBuildInfo.query2
    val SPARK_QUERY3  = SparkBuildInfo.query3
    val SPARK_QUERY4  = SparkBuildInfo.query4
    val SPARK_MASTER  = SparkBuildInfo.spark_master
    val SPARK_INPUTFORMAT              = SparkBuildInfo.spark_inputformat
    val SPARK_SNAPPYDATA_CONNECTION    = SparkBuildInfo.snappydata_connection
    val SPARK_SRC_DIR                  = SparkBuildInfo.spark_src_dir
    val SPARK_SNPPY_RESULT_FILE        = SparkBuildInfo.snappy_results_file
    val SPARK_CASSANDRA_KEYSPACE       = SparkBuildInfo.cassandra_keyspace
    val SPARK_CASSANDRA_TABLE          = SparkBuildInfo.cassandra_table
    val SPARK_CASSANDRA_RESULT_FILE    = SparkBuildInfo.cassandra_results_file
    val SPARK_CONNECTION_HOST          = SparkBuildInfo.spark_cassandra_connection_host
    val SPARK_CASSANDRA_AUTH_USERNAME  = SparkBuildInfo.spark_cassandra_auth_username
    val SPARK_CASSANDRA_AUTH_PASSWORD  = SparkBuildInfo.spark_cassandra_auth_password

}
