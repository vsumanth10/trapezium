/**
* Copyright (C) 2016 Verizon. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package main.com.verizon.bda.trapezium.examples.dal.spark.cassandra

import java.util.Properties
import com.verizon.bda.trapezium.dal.spark.cassandra.{DAOHelper, CassandraDAO}
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext};

/**
  * @author Faraz
  *  These are integration testwhich connects to real cluster
  *  The table used here like iprepuattion or ipreputation are just for testing
  *  and should not be confused
  * with tables in netintel keyspace.
  * This class is not supposed to be run in CI/CD pipeline and thats why marked as object with
  * static methods
  * spark.cassandra.connection.host should be changed for your cluster
  * so we can run it inside intelliJ/eclipse
  *
  */
object CassandraSparkDAOFunctionalExample extends App {
  @transient var sc: SparkContext = _
  @transient implicit var sqlContext: CassandraSQLContext = _
  val ipreputation = "sparkcassandra-connector/src/test/resources/ipreputation"
  val conf = new SparkConf().setAppName("MemSQL test").setMaster("local[1]").
    set("spark.cassandra.connection.host", "md-bdadev-54.verizon.com").
    set("spark.driver.allowMultipleContexts", "true");
  var properties: java.util.Properties = new Properties
  var activeNameNode: String = null
  var batchTime: Long = 0
  val SPLIT_SIZE = 10
  var eventMaxDateHour: String = ""

  val daoTestSchema = StructType(
    Seq(StructField("ipaddress", LongType, true),
      StructField("processdate", DateType, true),
      StructField("origipaddress", StringType, true),
      StructField("color", StringType, true),
      StructField("description", StringType, true),
      StructField("reason", StringType, true),
      StructField("notes", StringType, true)));



  sc = new SparkContext(conf)

  sqlContext = new CassandraSQLContext(sc)


  def readTest(): Unit = {
    val testDao = new CassandraDAO("netintel", "netaquity",
      List("md-bdadev-54.verizon.com", "md-bdadev-55.verizon.com", "md-bdadev-56.verizon.com"))
    val df = testDao.getAll()


    df.printSchema()
    val list: Array[Row] = df.filter("rowoctets='1683' and columnoctet=110").collect();
    assert(list.length > 0);
    // now test cassandra DAO via Java route


  }

  def writeTest(): Unit = {


    try {

      // now do distinc of all the records
      // TODO: can't seem to set host and other connection info after SparkContext is created

      val inputFilesRDD = sc.textFile(ipreputation)

      val distinctRDD = inputFilesRDD.distinct(SPLIT_SIZE)
      logger.info("total count of records for ipreputation "
        + distinctRDD.count())
      val basicDAO: CassandraDAO = new CassandraDAO("netintel", "ipreputation3",
        List("md-bdadev-54.verizon.com", "md-bdadev-55.verizon.com", "md-bdadev-56.verizon.com"))

      val iprepDataFrame = processRDD(distinctRDD, sqlContext);
      iprepDataFrame.show();
      val time1: Long = System.currentTimeMillis();
      basicDAO.write(iprepDataFrame);
      val time2: Long = System.currentTimeMillis();
      logger.info("time it took to persist " + (time2 -
        time1))

      // now read it back for validation
      // now try to read back from data written into temp database.

      val ipDataFrame: DataFrame = basicDAO.getAll()
      val resultList: Array[Row] = ipDataFrame.filter("origipaddress='222.186.21.49'").collect()

      assert(resultList.length >= 1)
      val row: Row = resultList(0);
      // now check few fields to make sure that we have right elements.
      assert(row.getAs("color").equals("white"))
      assert(row.getAs("description").equals("malicious host"))
    }


    catch {
      case ex: Exception =>
        logger.error("Error in insert", ex.getMessage)
      case exr: RuntimeException =>
        logger.error("Error in insert", exr.getMessage)

    }

  }

  /**
    * Convert RDD to dataframe with validate
    * schema
    * @param ipreputationRDD input snmp data
    * @return DataFrame (snmp_raw)
    */

  def processRDD(ipreputationRDD: org.apache.spark.rdd.RDD[(String)],
                 sqlContext: CassandraSQLContext): DataFrame = {
    val df = if (!ipreputationRDD.isEmpty()) {
      // DAOHelper.ip2Long(x(0))
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val row = ipreputationRDD.map(x => x.toString().split(",")).map { x =>
        Row(DAOHelper.ip2Long(x(0)),
          new java.sql.Date(format.parse(x(1).substring(0, 10)).getTime()),
          x(0), x(2), x(3), x(4),
          x(5));
      }
      // now do distinc of all the records

      sqlContext.createDataFrame(row, daoTestSchema)

    } else null
    df
  }
}

object DAOHelper extends Serializable {

  def ip2Long(ipAddress: String): Long = {
    val ipAddressInArray: Array[String] = ipAddress.split("\\.");
    var result: Double = 0

    var i: Int = 0
    while (i < ipAddressInArray.length) {
      val power: Int = 3 - i
      val ip: Int = ipAddressInArray(i).toInt
      result = result + ip * Math.pow(256, power)
      i = i + 1;
    }


    result.toLong;

  }
}
