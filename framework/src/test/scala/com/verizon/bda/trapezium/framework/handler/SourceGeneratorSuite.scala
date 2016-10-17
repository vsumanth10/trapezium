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
package com.verizon.bda.trapezium.framework.handler

import java.io.{BufferedWriter, FileWriter}
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.ApplicationConfig
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.zookeeper.EmbeddedZookeeper
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * @author sumanth.venkatasubbaiah
 *         tests for source generators
 */
class SourceGeneratorSuite extends FunSuite with LocalSparkContext with BeforeAndAfter {
  val numberOfBadRecordsMIDM = 7
  var zk: EmbeddedZookeeper = null
  var appConfig: ApplicationConfig = _

  before {
    appConfig = ApplicationManager.getConfig()

    zk = new EmbeddedZookeeper(appConfig.zookeeperList.split(",")(0))
  }

  after {
    // Close ZooKeeper connections
    ZooKeeperConnection.close

    if (zk != null) {
      zk.shutdown
    }
  }

  test("filesRDD method should return a map of string and RDD[Row] when reading a directory") {

    val workflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    ApplicationManager.updateWorkflowTime(System.currentTimeMillis())
    val path1 = "src/test/data/hdfs/source1/"
    val path2 = "src/test/data/hdfs/source2/"
    val sources = FileSourceGenerator(workflowConfig, appConfig, sc, 1)
    val rddMap = sources.next()
    val dfMap = sources.next()._2
    assert(2 == dfMap.size)
    assert(dfMap.contains("source1"))
    assert(dfMap.contains("source2"))
    assert(!dfMap.contains("txn1Output"))

    val source1 = sc.textFile(path1).map(Row(_))
    val source2 = sc.textFile(path2).map(Row(_))

    assert(source1.count - numberOfBadRecordsMIDM == dfMap("source1").count)
    assert(source2.count == dfMap("source2").count)
  }

  test("filesRDD method should return a map of string and RDD[Row] when reading list of files") {

    // Switch to new workflow
    val workflowConfig = ApplicationManager.setWorkflowConfig("testWorkFlow2")
    val conf = new Configuration
    val MTIME = 100
    ApplicationManager.updateWorkflowTime(System.currentTimeMillis())

    val path1 = "src/test/data/hdfs/testDataInsideDirs/"

    val tempDir = s"${path1}/temp"
    val fs = new RawLocalFileSystem
    fs.setConf(conf)
    fs.mkdirs(new Path(tempDir))

    // Create 10 new files
    ( 1 to 10 ).foreach { i => {
      val destFile = s"$tempDir/${i}.txt"
      fs.createNewFile(new Path(destFile))
      val br = new BufferedWriter(new FileWriter(destFile))
      br.write(s"this,is,${destFile}\n")
      br.close
    }
    }
    // Change the modify timestamp of 5 of these new files
    ( 1 to 10 ).filter{_%2==0}.foreach{ i => {
      val destFile = s"$tempDir/${i}.txt"
      fs.setTimes(new Path(destFile), MTIME, 1)
    }}

    val sources = FileSourceGenerator(workflowConfig, appConfig, sc, 1)
    val rddMap = sources.next()._2

    assert(1 == rddMap.size)
    assert(5 == rddMap("testDataInsideDirs").count)

    fs.delete(new Path(tempDir), true)
  }

  test("filesRDD method should return a null map when no files exist when reading list of files") {

    val workflowConfig = ApplicationManager.setWorkflowConfig("testWorkFlow1")
    ApplicationManager.updateWorkflowTime(System.currentTimeMillis())

    val sources = FileSourceGenerator(workflowConfig, appConfig, sc, 1)
    val rddMap = sources.next()._2

    assert(0 == rddMap.size)
    assert(!rddMap.contains("zeroFiles"))
  }

  test("Test onlydir = true with  numberofItr specified") {

    val workflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    val Itr = 2
    val sources = FileSourceGenerator(workflowConfig, appConfig, sc, Itr)
    var outPutItr = 1
    while (sources.hasNext()) {
      sources.next()
      outPutItr += 1
    }
    assert(Itr == outPutItr)
  }

}

