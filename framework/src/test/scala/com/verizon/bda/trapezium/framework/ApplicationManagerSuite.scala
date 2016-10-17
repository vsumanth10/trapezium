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
package com.verizon.bda.trapezium.framework

import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory
import scala.collection.mutable.{Map => MMap}
import scala.io.Source
import org.apache.spark.sql.Row
import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.{InetAddress, URI}
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
/**
 * @author sumanth.venkatasubbaiah Application Manager tests
 *         debasish83 cleanup test creation for runStreamWorkflow and runBatchWorkflow
 *         pankaj added test cases for running more than one workflow in single test case
 */
class ApplicationManagerSuite extends ApplicationManagerTestSuite {

  val logger = LoggerFactory.getLogger(this.getClass)

  test("getConfig should read the config file") {
    assert(appConfig.env == "local")
    assert(appConfig.sparkConfParam.getString("spark.akka.frameSize") == "100")
  }

  test("runStreamWorkFlow should successfully run the stream workflow") {
    val path1 = "src/test/data/hdfs/hdfs_stream_1.csv"
    val path2 = "src/test/data/hdfs/hdfs_stream_2.csv"

    val input1: Seq[String] = Source.fromFile(path1).mkString("").split("\n").toSeq
    val input2: Seq[String] = Source.fromFile(path2).mkString("").split("\n").toSeq

    val inputStream = Seq(input1, input2)

    val ssc = setupActionStreams(inputStream, (inputStream: DStream[String]) => {
      val inputRowStream = inputStream.map(line => Row(line))
      val workflowConfig = ApplicationManager.setWorkflowConfig("streamWorkFlow")

      ApplicationManager.updateWorkflowTime(System.currentTimeMillis())

      val hdfsConfig = workflowConfig.hdfsStream.asInstanceOf[Config]
      val streamsInfo = hdfsConfig.getList("streamsInfo")
      logger.info(s"STREAM ${streamsInfo.toString}")

      val dStreams = getValidatedDStream(streamsInfo, inputStream)

      ApplicationManager.runStreamWorkFlow(
        MMap("hdfsStream" -> dStreams("hdfsStream")))
    }, 2)
    runActionStreams(ssc, 2)
  }

  test("runBatchWorkFlow should successfully run the batch workflow") {
    val workFlow: WorkflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")

    ApplicationManager.updateWorkflowTime(System.currentTimeMillis())

    ApplicationManager.runBatchWorkFlow(
      workFlow,
      appConfig,
      maxIters = 2)(sc)
  }

  test("multiple workflows in single test"){

    val workflow1Name = "streamWorkFlow"
    val workflow2Name = "batchWorkFlow"

    val workflow1 = new WorkflowThread(workflow1Name)
    val workflow2 = new WorkflowThread(workflow2Name)

    workflow1.start
    workflow2.start

    while(!workflow1.isStarted && !workflow2.isStarted){

      logger.info(s"Sleeping because workflows are not yet started")
      Thread.sleep(1000)

    }

    workflow1.synchronized {
      workflow2.synchronized {

        // notify threads to read workflow name
        workflow1.notify
        workflow2.notify

      }
    }

    logger.info(s"Notifying threads to read workflow name")
    Thread.sleep(1000)

    workflow1.synchronized {
      workflow2.synchronized {

        assert(workflow1.getWorkflowName == workflow1Name
          && workflow2.getWorkflowName == workflow2Name)
      }

    }
  }

  test("runBatchWorkFlow should successfully onlyDir = true then run only 1") {
    val workFlow: WorkflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    ApplicationManager.runBatchWorkFlow(
      workFlow,
      appConfig )(sc)
  }

  test("read parquet file") {

    val workFlow: WorkflowConfig = ApplicationManager.setWorkflowConfig("readParquet")
    ApplicationManager.runBatchWorkFlow(
      workFlow,
      appConfig )(sc)
  }

  test("read avro file") {

    val workFlow: WorkflowConfig = ApplicationManager.setWorkflowConfig("readAvro")
    ApplicationManager.runBatchWorkFlow(
      workFlow,
      appConfig )(sc)
  }

  test("read json file") {

    val workFlow: WorkflowConfig = ApplicationManager.setWorkflowConfig("readJson")
    ApplicationManager.runBatchWorkFlow(
      workFlow,
      appConfig )(sc)
  }

  test("runBatchWorkFlow should successfully onlyDir = false then run only based on maxIters") {
    createFiles

    val workFlow: WorkflowConfig = ApplicationManager.setWorkflowConfig("onlyDirTrue")
    ApplicationManager.updateWorkflowTime(System.currentTimeMillis()-5000)

    ApplicationManager.runBatchWorkFlow(
      workFlow,
      appConfig,
      maxIters = 2 )(sc)
    fileDelete
  }

  def createFiles () : Unit = {
    val sourceMIDM = "src/test/data/hdfs/source1/file1.csv"
    val destinationMIDM = "src/test/data/hdfs/onlyDirTest/new_file" + System.currentTimeMillis() +
      "/midm.txt"
    val scrPath = new Path(sourceMIDM)
    val destinationPath = new Path(destinationMIDM)
    val conf = new Configuration
    val uri = new URI(sourceMIDM)
    val fsystem = FileSystem.get(uri, conf)
    fsystem.copyFromLocalFile(scrPath, destinationPath)

  }

  def fileDelete(): Unit = {
    val fs = new RawLocalFileSystem
    val destinationMIDM = "src/test/data/hdfs/onlyDirTest"
    val scrPath = new Path(destinationMIDM)
    val conf = new Configuration
    val uri = new URI(destinationMIDM)
    val fsystem = FileSystem.get(uri, conf)
    fsystem.delete(scrPath, true)
  }


  test("test registerhostname") {

    val workFlow: WorkflowConfig = ApplicationManager.setWorkflowConfig("batchWorkFlow")
    ApplicationManager.registerDriverHost("batchWorkFlow")
    val hostName = {
      try {
        InetAddress.getLocalHost().getHostName()
      } catch {
        case e: Exception =>
          "NA"
      }
    }
    val key = "/batchWorkFlow/driverHostName"
    val hostNAmeFromApplication = ApplicationUtils.getValFromZk(key , appConfig.zookeeperList)
    logger.info("driver host " + hostNAmeFromApplication )
    assert(hostNAmeFromApplication.equals(hostName))

  }

}

class WorkflowThread (val workflowName: String) extends Thread {

  val logger = LoggerFactory.getLogger(this.getClass)

  var isStarted: Boolean = false
  var localWorkflowName: String = _
  override def run(): Unit = {
    logger.info(s"Inside workflow $workflowName")
    this.synchronized {
      isStarted = true
      ApplicationManager.setWorkflowConfig(workflowName)
      logger.info(s"$workflowName going to wait state")
      this.wait
    }

    this.synchronized {
      localWorkflowName = ApplicationManager.getWorkflowConfig.workflow
      this.wait
    }

  }

  def getWorkflowName: String = {

    logger.info(s"localWorkflowName is $localWorkflowName")
    localWorkflowName
  }





}
