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
package com.verizon.bda.trapezium.dal.core.cassandra

import com.datastax.driver.core.{ResultSet, Cluster}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.verizon.bda.trapezium.dal.core.cassandra.utils.CassandraDAOUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable.ListBuffer
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import scala.collection.JavaConverters._

/**
  * Created by Faraz on 2/29/16.
  * These are unit tests which uses embeded dummy Cassandra.
  * The table used here like iprepuattion or ipreputation are just for testing
  * and should not be confused
  * with tables in netintel keyspace.
  * extends SparkFunSuite with LocalSparkContext
  */
class CassandraDAOUnitTest extends CassandraTestSuiteBase {

  var ipDao: CassandraDAO = null
  var ipDaoTTL: CassandraDAO = null
  // TODO: can't seem to set host and other connection info after SparkContext is created
  val conf = new SparkConf().setAppName("CassandraSQL test").setMaster("local[1]").
    set("spark.driver.allowMultipleContexts", "true");
  implicit val sc = new SparkContext(conf)
  var sqlContext: SQLContext = new SQLContext(sc);

  val cassandraDAOUtils: CassandraDAOUtils = new CassandraDAOUtils(
    ListBuffer("localhost"), "netintel", "ipreputation2");

  val cassandraDAOUtilsWithTTL: CassandraDAOUtils = new CassandraDAOUtils(
    ListBuffer("localhost"), "netintel", "ipreputationTTL");


  override def beforeAll() {
    super.beforeAll()
    setupDataInCassandra()

  }


  /**
    * This method data in embeded cassandra like
    * keyspaces, column familes and data
    */
  def setupDataInCassandra(): Unit = {

    try {

      executeSetupScript(String.format("CREATE KEYSPACE IF NOT EXISTS %s" +
        "  WITH REPLICATION = " +
        "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", "netintel"))

      executeSetupScript("CREATE TABLE netintel.ipreputation2 (  ipaddress  " +
        "bigint,  processdate  timestamp, origipaddress   varchar,  color    " +
        "varchar,  description    varchar,  reason  varchar," +
        "notes   varchar," +
        "PRIMARY KEY (ipaddress))")

      executeSetupScript("CREATE TABLE netintel.ipreputationTTL (  ipaddress  " +
        "bigint,  processdate  timestamp, origipaddress   varchar,  color    " +
        "varchar,  description    varchar,  reason  varchar," +
        "notes   varchar," +
        "PRIMARY KEY (ipaddress))")


      executeSetupScript(generateInsertStatment());

      val list: java.util.List[String] = new java.util.ArrayList[String]();
      list.add("localhost");
      ipDao = new CassandraDAO(ListBuffer("localhost"), "netintel", "ipreputation2");
      ipDaoTTL = new CassandraDAO(list, "netintel", "ipreputationTTL"
      );
      logger.info("ipDaoTTL = " + ipDaoTTL)


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

  /**
    * It reads data which we setup in setup embeded cassandra phase.
    */
  test("Cassandra IP DAO Read test") {

    val selectJava = ipDao.getAll()
    val resultJava: ResultSet = ipDao.execute(selectJava.where(
      QueryBuilder.eq("ipaddress", 1095549466L)));
    val resultList: java.util.List[com.datastax.driver.core.Row] = resultJava.all();
    logger.info("result 2 set found is "
      + resultList);
    assert(resultList.size() >= 1)
    val row: com.datastax.driver.core.Row = resultList.get(0);
    // now check few fields to make sure that we have right elements.
    assert(row.getString("color").equals("red"))
    assert(row.getString("notes").equals("Aliens"))


  }

  /**
    * It reads data which we setup in setup embeded cassandra phase.
    *  It uses selected columns only.
    */
  test("Cassandra IP DAO Read test for selected columns") {
  // def getColumns(cols: mutable.Buffer[String]): Select = {
    val buf: java.util.List[String] = new java.util.ArrayList[String]()
    buf.add("ipaddress")
    buf.add("color")
    buf.add("notes")
    val selectJava = ipDao.getColumns(buf.asScala)
    val resultJava: ResultSet = ipDao.execute(selectJava.where(
      QueryBuilder.eq("ipaddress", 1095549466L)));
    val resultList: java.util.List[com.datastax.driver.core.Row] = resultJava.all();
    logger.info("result 2 set found is "
      + resultList);
    assert(resultList.size() >= 1)
    val row: com.datastax.driver.core.Row = resultList.get(0);
    // now check few fields to make sure that we have right elements.
    assert(row.getString("color").equals("red"))
    assert(row.getString("notes").equals("Aliens"))


  }

  /**
    * Need to shutdown Cassandra and Embeded Cassandra. As SessionManager is
    * object it will shutdown for all instances of Cassandra DAOs
    */
  override def afterAll(): Unit = {
    super.afterAll()
    sc.stop();
  }


  test("Cassandra IP DAO Write test") {
    val ipreputation = "src/test/resources/ipreputation"
    val inputFilesRDD = sc.textFile(ipreputation, 2)
    logger.info("count of records in ipreputation is "
      + inputFilesRDD.count())
    cassandraDAOUtils.persistWithDataFrameToDaoAdapter(
      cassandraDAOUtils.processRDD2(inputFilesRDD, sqlContext));
    // now read back what we wrote for validation.
    val selectJava = ipDao.getAll()
    val resultJava: ResultSet = ipDao.execute(selectJava.where(
      QueryBuilder.eq("ipaddress", 1136098199L)));
    val resultList: java.util.List[com.datastax.driver.core.Row] = resultJava.all();
    logger.info("result 2 set found is "
      + resultList);
    assert(resultList.size() >= 1)
    val row: com.datastax.driver.core.Row = resultList.get(0);
    // now check few fields to make sure that we have right elements.
    assert(row.getString(1).equals("67.183.123.151"))
    assert(row.getString(2).equals("white"))

  }


  test("test number of sessions to tets session manager") {

    val cassandraDAOUtils: CassandraDAOUtils = new CassandraDAOUtils(
      ListBuffer("localhost"), "netintel", "ipreputation2");

    // we should still get total three sessions because uptill
    // this time we have used only 1 tablespace

    assert(SessionManager.sessions.length == 1)
    // there should be only one open session by this time

  }

  test("Calling shutdown multiple times should not blow. Test it") {

    SessionManager.shutdown()
    // call it again.
    SessionManager.shutdown()

  }


  /**
    * As it is called after shutdown, it will also test
    * that we can get session after shutdown
    */
  test("Cassandra IP DAO Write test With TTL ") {



    val ipreputation = "src/test/resources/ipreputation"
    val inputFilesRDD = sc.textFile(ipreputation, 2)
    logger.info(" count of records in ipreputation is "
      + inputFilesRDD.count())
    cassandraDAOUtilsWithTTL.persistWithDataFrameToDaoAdapterWithTTL(
      cassandraDAOUtilsWithTTL.processRDD2(inputFilesRDD, sqlContext), 10);
    // now read back what we wrote for validation.
    var selectJava = ipDaoTTL.getAll()
    var resultJava: ResultSet = ipDaoTTL.execute(selectJava.where(
      QueryBuilder.eq("ipaddress", 1136098199L)));
    var resultList: java.util.List[com.datastax.driver.core.Row] = resultJava.all();
    logger.info(" result 2 set found is "
      + resultList);
    assert(resultList.size() >= 1)
    val row: com.datastax.driver.core.Row = resultList.get(0);
    // now check few fields to make sure that we have right elements.
    assert(row.getString(1).equals("67.183.123.151"))
    assert(row.getString(2).equals("white"))

    // This test shutdown pool in between. our session logic should still work.
    // after shutdown.
    SessionManager.shutdown()

    // now wait 60 seconds. We should not get any records after 60 seconds.
    logger.info(" Going before println statement ")
    try {
      Thread.sleep(10000); // 60 seconds.
    }
    catch {
      case e: Exception => {
        logger.info("exception we got is " + e)
      }
    }

    selectJava = ipDaoTTL.getAll()
    resultJava = ipDaoTTL.execute(selectJava.where(
      QueryBuilder.eq("ipaddress", 1136098199L)));
    resultList = resultJava.all();
    logger.info(" result 2 set found is "
      + resultList);
    assert(resultList.size() == 0)

  }


  /**
    * helper method to transform an ip address from string like 123.345.868.876 to long.
    * @param ipAddress
    * @return
    */
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

  /**
    * Hard coded insert statement for setup purpose. We dont want to use "write"
    * method of DAO for this initial setup for
    * indepdendence of write and read tests
    * @return
    */
  private def generateInsertStatment(): String = {

    val query: String = "INSERT INTO netintel.ipreputation2 " +
      "( ipaddress, " +
      "origipaddress, color " +
      " ,description ,  reason ," +
      " processdate ,  notes )  " +
      " VALUES ( 1095549466 , '1095549466' " +
      ", 'red' ,'bad ip' , 'attacker',  '2012-10-2 12:10' ,  'Aliens' )"

    return query;
  }


}
