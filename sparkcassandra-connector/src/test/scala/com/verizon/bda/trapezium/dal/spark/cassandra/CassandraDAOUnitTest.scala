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
package com.verizon.bda.trapezium.dal.spark.cassandra

import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import com.verizon.bda.trapezium.dal.core.cassandra.CassandraTestSuiteBase

/**
  * Created by Faraz on 2/29/16.
  * It does simple write and read test using spark connector
  * It uses embeded Cassandra.
  * The table used here like iprepuattion or ipreputation are just for testing and should not
  * be confused
  * with tables in netintel keyspace.
  */
class CassandraDAOUnitTest extends CassandraTestSuiteBase {

  var ipDao: CassandraDAO = null
  // TODO: can't seem to set host and other connection info after SparkContext is created
  val conf = new SparkConf().setAppName("CassandraSQL test").setMaster("local[1]")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.driver.allowMultipleContexts", "true")

  implicit val sc = new SparkContext(conf)
  implicit var sqlContext: CassandraSQLContext = new CassandraSQLContext(sc)

  val daoTestSchema = StructType(
    Seq(StructField("ipaddress", LongType, true),
      StructField("origipaddress", StringType, true),
      StructField("color", StringType, true),
      StructField("description", StringType, true),
      StructField("reason", StringType, true),
      StructField("processdate", DateType, true),
      StructField("notes", StringType, true)));

  val cassandraDAOUtils: CassandraDAOUtils = new CassandraDAOUtils(
    List("localhost"), "netintel", "ipreputation2", sqlContext);

  override def beforeAll() {
    super.beforeAll()
    setupDataInCassandra();
  }


  /**
    * This method data in embeded cassandra like
    * keyspaces, column familes and data
    * Even We are using same name as tables in netintel, these are just for testing.
    */
  def setupDataInCassandra(): Unit = {
    try {
      executeSetupScript(String.format("CREATE KEYSPACE IF NOT EXISTS %s" +
        "  WITH REPLICATION = " +
        "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", "netintel"))

      executeSetupScript("CREATE TABLE netintel.ipreputation2 (  ipaddress  " +
        "bigint,  origipaddress   varchar,  color    " +
        "varchar,  description    varchar,  reason  varchar," +
        "processdate  timestamp,  notes   varchar," +
        "PRIMARY KEY (ipaddress))")


      val query: String = generateInsertStatment("ipreputation2", "red");
      executeSetupScript(query);

      ipDao = new CassandraDAO("netintel", "ipreputation2", List("localhost"));


    }
    catch {
      case e: Exception => {
        logger.error("exception we got is " , e.getMessage)
      }
      case err: Throwable => {
        logger.error("exception we got is " , err.getMessage)
      }
    }

  }

  test("Cassandra IP DAO Read test") {


    val ipDataFrame: DataFrame = ipDao.getAll()
    val resultList: Array[Row] = ipDataFrame.filter("origipaddress='1095549466'").collect()
    logger.info("result 2 set found is "
      + resultList);
    assert(resultList.length >= 1)
    val row: Row = resultList(0);
    // now check few fields to make sure that we have right elements.
    assert(row.getAs("color").equals("red"))
    assert(row.getAs("notes").equals("Aliens"))

  }

  test("Cassandra IP DAO Read test With Explict Table") {

    val ipDataFrame: DataFrame = ipDao.getAll()
    val resultList: Array[Row] = ipDataFrame.filter("origipaddress='1095549466'").collect()
    logger.info("result 2 set found is "
      + resultList);
    assert(resultList.length >= 1)
    val row: Row = resultList(0);
    // now check few fields to make sure that we have right elements.
    assert(row.getAs("color").equals("red"))
    assert(row.getAs("notes").equals("Aliens"))

  }

  /**
    * It tests if we can provide extra options params
    */
  test("Cassandra Config Options Test") {

    // create two DAOs one with default options and one wiyth additionbal options.
    // Check size of options

    val ipDaoForConfig2 = new CassandraDAO("netintel", "ipreputation2", List("localhost"));

    assert(ipDaoForConfig2.getOptions().size == 2)
     // now create one with additional options

    val ipDaoForConfig4 = new CassandraDAO("netintel", "ipreputation2", List("localhost"),
      Map("spark_cassandra_output_metrics" -> "false",
        "spark_cassandra_output_concurrent_writes" -> "20"));

    assert(ipDaoForConfig4.getOptions().size == 4)

  }


  override def afterAll(): Unit = {
    super.afterAll()
  }

  def transformToList(row: Row): List[Object] = {

    val time1: Long = System.currentTimeMillis();
    val walker: Iterator[Any] = row.toSeq.toIterator
    val rowList = new ListBuffer[Object];
    while (walker.hasNext) {

      var item: Object = walker.next().asInstanceOf[Object];
      rowList += item

    }
    val time2: Long = System.currentTimeMillis();

    return rowList.toList;
  }

  test("Cassandra IP DAO Write test") {
    val ipreputation = "src/test/resources/ipreputation"
    val inputFilesRDD = sc.textFile(ipreputation, 1)
    logger.info("count of records in ipreputation is "
      + inputFilesRDD.count())
    ipDao.write(cassandraDAOUtils.processRDD(inputFilesRDD, sqlContext))


    // now try to read back from data written into temp database.
    val ipDataFrame: DataFrame = ipDao.getAll()
    val resultList: Array[Row] = ipDataFrame.filter("origipaddress='222.186.21.49'").collect()

    assert(resultList.length >= 1)
    val row: Row = resultList(0);
    // now check few fields to make sure that we have right elements.
    assert(row.getAs("color").equals("white"))
    assert(row.getAs("description").equals("malicious host"))

  }

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

  private def generateInsertStatment(table: String, color: String): String = {

    val query: String = "INSERT INTO netintel." + table  +
      "( ipaddress, " +
      "origipaddress, color " +
      " ,description ,  reason ," +
      " processdate ,  notes )  " +
      " VALUES ( 1095549466 , '1095549466' " +
      ", '" + color + "' ,'bad ip' , 'attacker',  '2012-10-2 12:10' ,  'Aliens' )"

    return query;
  }


}
