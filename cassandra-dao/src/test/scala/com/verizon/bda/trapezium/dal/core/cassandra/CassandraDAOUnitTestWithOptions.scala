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

import java.util

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{ConsistencyLevel, ResultSet, Cluster}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import java.util.List;
import collection.JavaConverters._

/**
  * Created by Faraz on 2/29/16.
  * These are unit tests which uses embeded dummy Cassandra.
  * The table used here like iprepuattion or ipreputation are just for testing
  * and should not be confused
  * with tables in netintel keyspace.
  * extends SparkFunSuite with LocalSparkContext
  */


import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by v468328 on 5/2/16.
  */
class CassandraDAOUnitTestWithOptions  extends CassandraTestSuiteBase {


  var basicDAO: CassandraDAO = null

/* TODO: fix the issue in the embedded cassandra unit test
  override def beforeAll() {
    super.beforeAll()
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("another-cassandra2.yaml");
    setupDataInCassandra();
  }


  /**
    * This method data in embeded cassandra like
    * keyspaces, column familes and data
    */
  def setupDataInCassandra(): Unit = {

    try {

      var cluster: Cluster = Cluster.builder()
        .addContactPoint("localhost")
        .withPort(9140)
        .build();
      cluster.connect().execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s" +
        "  WITH REPLICATION = " +
        "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", "netintel"));


      cluster.connect().execute("CREATE TABLE netintel.ipreputation2 (  ipaddress  " +
        "bigint,  processdate  timestamp, origipaddress   varchar,  color    " +
        "varchar,  description    varchar,  reason  varchar," +
        "notes   varchar," +
        "PRIMARY KEY (ipaddress))");


      val query: String = generateInsertStatment();
      cluster.connect().execute(query);

      val map: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()
      map.put("port", 9140.asInstanceOf[Integer])
      map.put("consistencyLevel", ConsistencyLevel.QUORUM)
      val cassandraConfig: CassandraConfig = new CassandraConfig(map);
      val list: java.util.List[String] = new java.util.ArrayList[String]
      list.add("localhost")
      // mutable.Buffer("localhost")

      basicDAO = new CassandraDAO( list.asScala, "netintel",
        "ipreputation2", cassandraConfig);

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


    val selectJava = basicDAO.getAll()
    val resultJava: ResultSet = basicDAO.execute(selectJava.where(
      QueryBuilder.eq("ipaddress", 1095549466)));
    val resultList: java.util.List[com.datastax.driver.core.Row] = resultJava.all();
    logger.info("result 2 set found is "
      + resultList);
    assert(resultList.size() >= 1)


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
*/

}
