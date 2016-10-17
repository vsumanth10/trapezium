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
package main.com.verizon.bda.trapezium.examples.dal.core.cassandra

import java.util

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.verizon.bda.trapezium.dal.core.cassandra.CassandraDAO
import com.verizon.bda.trapezium.dal.core.cassandra.utils.CassandraDAOUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * These are integration tests which test write and read operation while
  * using real cassandra clsuter.
  * The table used here like iprepuattion or ipreputation are just for testing and
  * should not be confused
  * with tables in netintel keyspace.
  * They are not supposed to be run in CI/CD pipeline thats why I have marked them as object so
  * we can run them from intelliJ/eclipse but thye will not run in mvn clean install
  * spark.cassandra.connection.host should be changed for your cluster
  */
object CassandraCoreDAOFunctionalExample extends App {


  var sqlContext: SQLContext = null;
  val ipDao = new CassandraDAO(ListBuffer("md-bdadev-54.verizon.com", "md-bdadev-55.verizon.com",
    "md-bdadev-56.verizon.com"), "netintel", "ipreputation");
  val netequityDao = new CassandraDAO(ListBuffer("md-bdadev-54.verizon.com",
    "md-bdadev-55.verizon.com", "md-bdadev-56.verizon.com"), "netintel", "netaquity");

  val cassandraDAOUtils: CassandraDAOUtils = new CassandraDAOUtils(
    ListBuffer("md-bdadev-54.verizon.com", "md-bdadev-55.verizon.com",
      "md-bdadev-56.verizon.com"), "netintel", "ipreputation5");
  // TODO: can't seem to set host and other connection info after SparkContext is created

  val conf = new SparkConf().setAppName("CassandraSQL test").setMaster("local[1]")
    .set("spark.cassandra.connection.host", "md-bdadev-54.verizon.com").
    set("spark.driver.allowMultipleContexts", "true");


  implicit val sc: SparkContext = new SparkContext(conf)


  val select = ipDao.getAll()
  val hostList: java.util.List[String] = new util.LinkedList[String]();
  hostList.add("md-bdadev-54.verizon.com");
  hostList.add("md-bdadev-55.verizon.com");
  sqlContext = new SQLContext(sc);


  /**
    * This is integration test and not supposed to be run from CI/CD pipeline
    */

  def readTest(): Unit = {
    val selectJava = ipDao.getAll()
    val resultJava = ipDao.execute(selectJava.where(QueryBuilder.eq("ipaddress", 2013915352L)));
    val resultList: java.util.List[com.datastax.driver.core.Row] = resultJava.all();
    logger.info("result 2  set found is => " + resultList)
    assert(resultJava != null)
    assert(resultList.asScala.length > 1);
  }

  def readTestAdvanced(): Unit = {
    val selectTraffic = netequityDao.getAll();
    val resultTraffic = netequityDao.execute(selectTraffic.where(
      QueryBuilder.eq("rowoctets", "22057")));
  }

  /**
    * This is integration test and not supposed to be run from CI/CD pipeline
    */
  def WriteTest(): Unit = {

    val ipreputation = "src/test/resources/ipreputation"
    val inputFilesRDD = sc.textFile(ipreputation)

    cassandraDAOUtils.persistWithDataFrameToDaoAdapter(
      cassandraDAOUtils.processRDD(inputFilesRDD, sqlContext));

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
    assert(row.getString(2).equals("white"))
  }


}

