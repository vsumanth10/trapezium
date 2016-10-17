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
/**
 * Copyright (C) Verizon Corp.
 */
package com.verizon.bda.trapezium.dal.sql

import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.Finders
import org.scalatest.FunSuite

/**
 * @author pramod.lakshminarasimha
 */
class HiveDAOTest extends FunSuite with MLlibTestSparkContext {
  @transient implicit var hiveContext: HiveContext = _
  @transient var testdf: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    hiveContext = new TestHiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val rdd: RDD[Row] = sc.parallelize(
      (1 to 10).map(x => new GenericRow(Array("a" + x, "b", x))).toList
        ++ (1 to 10).map(x => new GenericRow(Array("c" + x, "d", x))).toList)
    val schema: StructType = StructType(Array(
      StructField("c1", StringType, true),
      StructField("c2", StringType, true),
      StructField("c3", IntegerType, true)))
    testdf = hiveContext.createDataFrame(rdd, schema)
    testdf.registerTempTable("test_read")
  }

  override def afterAll() {
    hiveContext.asInstanceOf[TestHiveContext].reset()
    super.afterAll()
  }

  test("HiveDAO read test") {
    val testDao = new HiveDAO("default", "test_read")

    assert(testDao.getAll().count == 20)
    assert(testDao.getColumns(List("c1")).count == 20)
  }

  test("HiveDAO partitioned write test") {
    val tableName = "test_write_partitioned"
    hiveContext.sql(s"""create table if not exists ${tableName}
                  (c1 string) partitioned by (c2 string, c3 int)""")
    val testDao = new HiveDAO("default", tableName)

    testDao.write(testdf, Seq("c2", "c3"))
    assert(hiveContext.table(tableName).count() == 20)
    assert(hiveContext.table(tableName).where("c2 = 'b'").count() == 10)

    testDao.write(testdf, Seq("c2", "c3"))
    assert(hiveContext.table(tableName).count() == 40)
    assert(hiveContext.table(tableName).where("c2 = 'b'").count() == 20)

    hiveContext.sql(s"drop table ${tableName}")
  }

  test("HiveDAO delete partitions test") {
    val tableName = "test_delete_partitions"
    val testDao = new HiveDAO("default", tableName)

    hiveContext.sql(s"""create table if not exists ${tableName}
                  (c1 string) partitioned by (c2 string, c3 int)""")
    testDao.write(testdf, Seq("c2", "c3"))
    testDao.deletePartition("c2 = 'b'")
    assert(hiveContext.table(tableName).where("c3 = 1")
      .select("c1").count() == 1)
    testDao.deletePartitions(Array("c2 = 'd', c3 = 1",
      "c2 = 'd', c3 = 2",
      "c2 = 'd', c3 = 3",
      "c2 = 'd', c3 = 4",
      "c2 = 'd', c3 = 5"))
    assert(hiveContext.table(tableName).where("c2 = 'd'").count() == 5)
    hiveContext.sql(s"drop table ${tableName}")
  }

  test("Switch context test") {
    val tableName = "test_switch_context"
    val newSqlContext = new SQLContext(sc)
    val schema =
      StructType(
        StructField("name", StringType, false) ::
          StructField("age", IntegerType, true) :: Nil)

    val people =
      sc.parallelize(Seq("a,1", "b,2")).map(
        _.split(",")).map(p => Row(p(0), p(1).trim.toInt))

    val dataFrame = newSqlContext.createDataFrame(people, schema)

    dataFrame.registerTempTable(tableName)
    val testsqldf = newSqlContext.table(tableName)
    hiveContext.sql(s"create table if not exists ${tableName} (name string, age int)")
    val testDao = new HiveDAO("default", tableName)(hiveContext)
    testDao.write(testsqldf)
    assert(hiveContext.tableNames().contains(tableName) && testDao.getAll().count() == 2)
    hiveContext.sql(s"drop table ${tableName}")
  }
}
