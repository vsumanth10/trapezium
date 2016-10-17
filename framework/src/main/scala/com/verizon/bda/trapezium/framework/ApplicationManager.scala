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

import _root_.kafka.common.TopicAndPartition
import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.handler.{BatchHandler, FileSourceGenerator, SeqSourceGenerator, StreamingHandler}
import com.verizon.bda.trapezium.framework.hdfs.HdfsDStream
import com.verizon.bda.trapezium.framework.kafka.KafkaDStream
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, WorkflowConfig}
import com.verizon.bda.trapezium.framework.server.{AkkaHttpServer, EmbeddedHttpServer, JettyServer}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser
import java.net.InetAddress
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}

/**
 * @author Pankaj on 9/1/15.
 *         debasish83: added getConfig API, updated handleWorkflow API for vertical tests
 *         sumanth: Converted ApplicationManager into a Driver program
 *                  Added scopt for parsing command line args
 *                  Refactored handleWorkflow API to runStreamHandler API
 *                  Added runBatchHandler API
  *        Hutashan: Added changes for batch and return code
  *
 */

object ApplicationManager {

  val logger = LoggerFactory.getLogger(this.getClass)

  private var ssc: StreamingContext = null
  var sqlContext: SQLContext = null
  private var appConfig: ApplicationConfig = _
  private val threadLocalWorkflowConfig = new ThreadLocal[WorkflowConfig]();
  var stopStreaming: Boolean = false
  val ERROR_EXIT_CODE = -1
  private var embeddedServer: EmbeddedHttpServer = _


  def getEmbeddedServer: EmbeddedHttpServer = {

    embeddedServer
  }

  /**
   *
   * @param configDir input config directory
   * @return an instance of ApplicationConfig
   */
  def getConfig(configDir: String = null): ApplicationConfig = {
    if (appConfig == null) {
      logger.info(s"Application is null.")
      appConfig = new ApplicationConfig(ApplicationUtils.env, configDir)
    }

    appConfig
  }

  def setWorkflowConfig(workflow: String): WorkflowConfig = {

    val workflowConfig = new WorkflowConfig(workflow)

    threadLocalWorkflowConfig.set(workflowConfig)
    workflowConfig
  }

  /**
   *
   * @return
   */
  def getWorkflowConfig(): WorkflowConfig = {

    threadLocalWorkflowConfig.get
  }

  // Case class to hold the command line arguments
  private case class Params(configDir: String = null,
                            workFlowName: String = null)

  // ApplicationManager entry point
  def main(args: Array[String]) {

   try {
     val defaultParams = Params()

     val parser = new OptionParser[Params]("ApplicationManager") {
       head("ApplicationManager: BDA Spark Application runner")
       opt[String]("config")
         .text(s"local config directory path")
         .optional
         .action((x, c) => c.copy(configDir = x))
       opt[String]("workflow")
         .text(s"workflow to run")
         .required
         .action((x, c) => c.copy(workFlowName = x))
     }

     parser.parse(args, defaultParams).map { params =>
       run(params)
     } getOrElse {
       logger.error("Insufficient number of arguments", getUsage)
       System.exit(ERROR_EXIT_CODE)
     }
   } catch {
     case ex: Throwable => {
       logger.error(s"Exiting job because of following exception", ex)
       System.exit(ERROR_EXIT_CODE)
     }

   }
  }

  private def run(params: Params): Unit = {

    getConfig(params.configDir)
    val workFlowToRun = params.workFlowName

    // load start up class
    initialize(appConfig)




    val workflowConfig: WorkflowConfig = setWorkflowConfig(workFlowToRun)
    val currentWorkflowName = ApplicationManager.getWorkflowConfig.workflow
    registerDriverHost(currentWorkflowName)
    val runMode = workflowConfig.runMode
    runMode match {
      case "STREAM" => {

        initStreamThread (workFlowToRun)

      }
      case "BATCH" => {

        val dataSource = workflowConfig.dataSource

        dataSource match {

          case "KAFKA" => {

            initStreamThread (workFlowToRun)
          }
          case _ => {

            val sc = new SparkContext(getSparkConf (appConfig))
            runBatchWorkFlow(workflowConfig, appConfig)(sc)

            // if spark context is not stopped, stop it
            if( !sc.isStopped ){
              sc.stop
            }

          }
        }

      }
      case "API" => {

        val sc = new SparkContext(getSparkConf (appConfig))
        startHttpServer(sc, workflowConfig)

      }
      case _ => logger.error("Not implemented run mode. Exiting.. ", runMode)
    }
  }

  private[framework] def initStreamThread (streamWorkflowName: String): Unit = {

    while (!stopStreaming) {
      val workflowth = new StreamWorkflowThread (streamWorkflowName)

      workflowth.start()

      this.synchronized {
        logger.info(s"Waiting to receive notification to restart streaming context")
        this.wait()

        if( stopStreaming ) {

          logger.info(s"Received notification to stop streaming context")
        } else {

          logger.info(s"Received notification to restart streaming context")
        }
        // Stop current workflow thread before re-creating a new workflow thread
        workflowth.interrupt

      }

      if( ssc != null && ssc.getState() != StreamingContextState.STOPPED) {

        logger.info(s"Do we need to stop spark context? ${stopStreaming}")

        try {

          // Stop the current streaming context
          ssc.stop(stopStreaming, false)


        } catch {
          case ex: Throwable => {

            logger.error(s"Consumed following exception because " +
              s"spark context was NOT stopped gracefully." , ex)
            throw ex
          }

        }
      }

      ssc = null

    }

  }

  private[framework] def initStreamWorkflow: Unit = {

    logger.info("Starting Streaming WorkFlow")

    val workflowConfig = ApplicationManager.getWorkflowConfig
    var dStreams: MMap[String, DStream[Row]] = null
    val dataSource = workflowConfig.dataSource
    logger.info(s"Running in stream mode with $dataSource datasource")

    val sparkConf: SparkConf = getSparkConf(appConfig)
    val runMode = workflowConfig.runMode

    dataSource match {
      case "HDFS" => {
        val minRememberDuration =
          ApplicationUtils.calculateHdfsRememberDuration

        val hdfsStreamConfig = workflowConfig.hdfsStream.asInstanceOf[Config]
        val checkPointDirectory = hdfsStreamConfig.getString("checkpointDirectory")

        logger.info(s"Go back by $minRememberDuration secs")
        // sparkConf.set("spark.streaming.fileStream.minRememberDuration",
        // s"${minRememberDuration}")
        sparkConf.set("spark.streaming.minRememberDuration", s"${minRememberDuration}s")

        ssc = HdfsDStream.createStreamingContext(hdfsStreamConfig.getString("batchTime").toInt,
          checkPointDirectory,
          sparkConf)
        dStreams = HdfsDStream.createDStreams(ssc)
      }
      case "KAFKA" => {
        val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
        val streamsInfo = kafkaConfig.getConfigList("streamsInfo")
        val kafkaBrokerList = appConfig.kafkabrokerList

        logger.info("Kafka broker list " + kafkaBrokerList)

        ssc = KafkaDStream.createStreamingContext(sparkConf)

        val topicPartitionOffsets = MMap[TopicAndPartition, Long]()

        streamsInfo.asScala.foreach(streamInfo => {

          val topicName = streamInfo.getString("topicName")

          val partitionOffset = KafkaDStream.fetchPartitionOffsets(topicName, runMode, appConfig)
          topicPartitionOffsets ++= partitionOffset

          val validConfig = streamInfo.getConfig("validation")
          dStreams = KafkaDStream.createDStreams(
            ssc,
            kafkaBrokerList,
            kafkaConfig,
            topicPartitionOffsets.toMap,
            appConfig)
        })
      }

      case _ => {
        logger.error("Mode not implemented. Exiting...", dataSource)
        System.exit(ERROR_EXIT_CODE)
      }
    }

    runStreamWorkFlow(dStreams)
    addStreamListeners(ssc, workflowConfig)
  }

  /**
   * method to create a SparkContext
   *
   * @return SparkContext object
   */
  private[framework] def getSparkConf(appConfig: ApplicationConfig): SparkConf = {

    val sparkConfigParam: Config = appConfig.sparkConfParam
    val sparkConf = new SparkConf
    sparkConf.setAppName(appConfig.appName)

    if (!sparkConfigParam.isEmpty) {
      val keyValueItr = sparkConfigParam.entrySet().iterator()
      while (keyValueItr.hasNext) {
        val sparkConfParam = keyValueItr.next()
        sparkConf.set(sparkConfParam.getKey, sparkConfParam.getValue.unwrapped().toString)
        logger.info(s"${sparkConfParam.getKey}: ${sparkConfParam.getValue.unwrapped().toString}")
      }
    }
    if (appConfig.env == "local") sparkConf.setMaster("local[5]")

    sparkConf
  }

  private[framework] def initialize(appConfig: ApplicationConfig): Unit = {
    // load start up class
    val applicationStartupClass: String = appConfig.applicationStartupClass
    val startupClass = ApplicationUtils.getStartupClass(applicationStartupClass)
    startupClass.init(appConfig.env, appConfig.configDir, appConfig.persistSchema)
  }

  /**
   * Start HTTP (Jetty) server
   *
   * @param workflowConfig
   */
  private[framework] def startHttpServer(sc: SparkContext, workflowConfig: WorkflowConfig): Unit = {

    val serverConfig = workflowConfig.httpServer
    if (serverConfig != null) {
      val provider = serverConfig.getString("provider")
      embeddedServer = provider match {
        case "akka" => new AkkaHttpServer(sc)
        case "jetty" => new JettyServer(sc, serverConfig)
      }
      logger.info(s"Starting $provider based Embedded HTTP Server")
      embeddedServer.init(serverConfig)
      embeddedServer.start(serverConfig)
    }
  }

  /**
   * Method to print the usage of this Application
   *
   * @return
   */
  private def getUsage: String = {
    "ApplicationManager --config <configDir> --workflow <workFlow>"
  }

  /**
   * Public API for running Stream workflow
   *
   * @param dStreams input dstreams
   */
  def runStreamWorkFlow(dStreams: MMap[String, DStream[Row]]) : Unit = {
    StreamingHandler.handleWorkflow(dStreams)
  }

  /**
   * Public API for adding listener to StreamingContext
   *
   * @param ssc StreamingContext
   * @param workflowConfig ApplicationConfig object
   */
  def addStreamListeners(ssc: StreamingContext,
                         workflowConfig: WorkflowConfig) : Unit = {

    startHttpServer(ssc.sparkContext, workflowConfig)

    StreamingHandler.addStreamListener(ssc, workflowConfig)
  }

  def runBatchWorkFlow(workFlow: WorkflowConfig,
                       appConfig: ApplicationConfig,
                       batches: Seq[Map[String, DataFrame]])
                      (implicit sc: SparkContext): Unit = {
    val sources = SeqSourceGenerator(workFlow, appConfig, sc, batches)
    BatchHandler.scheduleBatchRun(workFlow, sources, appConfig, sc)
  }

  def runBatchWorkFlow(workFlow: WorkflowConfig,
                       appConfig: ApplicationConfig,
                       maxIters: Long = -1)
                      (implicit sc: SparkContext): Unit = {
    val sources = FileSourceGenerator(workFlow, appConfig, sc, maxIters)
    startHttpServer(sc, workFlow)
    BatchHandler.scheduleBatchRun(workFlow, sources, appConfig, sc)
  }

  def getSynchronizationTime: String = {

    val workflowConfig = ApplicationManager.getWorkflowConfig
    val synchronizationTime = ApplicationUtils.getSyncWorkflowWorkflowTime(
      appConfig, workflowConfig)

    synchronizationTime match {

      case Some(value) => {

        value.toString
      }
      case None => null

    }
  }

  def updateWorkflowTime(timeStamp: Long, workflowName: String = ""): Unit = {

    val currentWorkflowName =
      if ( workflowName.size == 0) {
        ApplicationManager.getWorkflowConfig.workflow
      } else {
        workflowName
      }

    ApplicationUtils.updateCurrentWorkflowTime(
      currentWorkflowName,
      timeStamp,
      appConfig.zookeeperList)
  }


  def updateSynchronizationTime(timeStamp: Long, workflowName: String = ""): Unit = {

    val currentWorkflowName =
      if ( workflowName.size == 0) {
        ApplicationManager.getWorkflowConfig.workflow
      } else {
        workflowName
      }

    ApplicationUtils.updateSynchronizationTime(
      currentWorkflowName,
      timeStamp,
      appConfig.zookeeperList)
  }


   def registerDriverHost(currentWorkflowName : String) : Unit = {
    val hostName = {
      try {
        InetAddress.getLocalHost().getHostName()
      } catch {
        case e: Exception =>
          "NA"
      }
    }
     logger.info("Registering hostName:" + hostName)

     ApplicationUtils.registerHostName(
      currentWorkflowName,
       hostName,
      appConfig.zookeeperList)
  }


  /**
   * method to read the kafka config params if any and return a Map[String, Object]
   * @param appConf the ApplicationConfig instance
   * @return a Map[String, Object]
   */
  def getKafkaConf(appConf: ApplicationConfig = null): Map[String, Object] = {

    val kafkaConfigParam: Config = if ( appConf != null ) {
      appConf.kafkaConfParam
    }
    else {
      appConfig.kafkaConfParam
    }

    val tempConf = scala.collection.mutable.Map[String, String]()

    if (!kafkaConfigParam.isEmpty) {
      val keyValueItr = kafkaConfigParam.entrySet().iterator()
      while (keyValueItr.hasNext) {
        val confParam = keyValueItr.next()
        tempConf += (confParam.getKey -> confParam.getValue.unwrapped().toString)

        logger.info(s"${confParam.getKey}: ${confParam.getValue.unwrapped().toString}")
      }
    }
    tempConf.toMap[String, Object]
  }
}

class StreamWorkflowThread (streamWorkflowName: String) extends Thread {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def run() : Unit = {
    logger.info(s"Starting a new workflow")

    try {
      ApplicationManager.setWorkflowConfig(streamWorkflowName)
      ApplicationManager.initStreamWorkflow

    } catch {

      case ex: InterruptedException => {

        logger.info("Thread interrupted to start another thread.")
      }
      case ex: Throwable => {

        logger.error("Stopping job", ex)
        ApplicationManager.stopStreaming = true
        throw ex
      }
    }

    ApplicationManager.synchronized {
      ApplicationManager.notifyAll()

    }
  }
}
