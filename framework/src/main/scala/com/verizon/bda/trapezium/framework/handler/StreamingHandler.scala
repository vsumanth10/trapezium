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

import java.sql.Time

import com.typesafe.config.ConfigObject
import com.verizon.bda.trapezium.framework.manager.{ApplicationConfig, ApplicationListener, WorkflowConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.{ApplicationManager, StreamingTransaction}
import com.verizon.bda.trapezium.validation.DataValidator
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory;

import scala.collection.mutable.{Map => MMap}

/**
 * @author pankaj.rastogi
 *    Originally ApplicationManager.scala by pankaj.rastogi and debasish83
 *    Handles the streaming workflows
 *    Modified by sumanth.venkatasubbaiah as a handler object
 */

/**
 * StreamingHandler
 * - decides the source of stream, e.g., HDFS or Kafka
 * - creates streamingcontext for all HDFS locations or Kafka topics
 * - Reads the Workflow and executes the transactions in the workflow
 * - adds shutdown hook to gracefully terminate the JVM
 */

private[framework] object StreamingHandler {

  private var isShutdownHookAdded = false
  private var ssc: StreamingContext = null
  private var currentWorkflowTime: Time = _
  val logger = LoggerFactory.getLogger(this.getClass);

  def addStreamListener(sc: StreamingContext,
                        workflowConfig: WorkflowConfig): Unit = {

    ssc = sc

    if( !isShutdownHookAdded) {
      // add shutdown hook so that we can complete the current batch before the JVM gets killed
      addShutdownHook()
      isShutdownHookAdded = true
    }

    // add streaming listener
    val listener: ApplicationListener = new ApplicationListener(workflowConfig)
    ssc.addStreamingListener(listener)

    ssc.start
    ssc.awaitTermination
  }

  def handleWorkflow(dStreams: MMap[String, DStream[Row]]): Unit = {

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.getWorkflowConfig

    // set currentWorkflowTime
    currentWorkflowTime = new Time(
      ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig))

    logger.info(s"Overall Stream collection $dStreams")
    val (workflowDStreams) = handleTransactions(dStreams)
    val workflowClassMap = MMap[String, StreamingTransaction]()

    logger.info("Persisting algorithm streams...")
    saveDStreams(workflowClassMap, workflowDStreams, appConfig)
  }

  /**
   * create workflow DAG from Typesafe Config files
   * persist transformed RDDs if isPersist command line argument is true
   * persist anomaly result RDDs if isPersist command line argument is false
   * @param dStreams
   */
  private def handleTransactions
  (dStreams: MMap[String, DStream[Row]]):
  MMap[String, DStream[Row]] = {

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.getWorkflowConfig
    val transactionsList = workflowConfig.transactions
    val transactionsListItr = transactionsList.iterator
    while (transactionsListItr.hasNext) {
      val transaction = transactionsListItr.next
      var workflowDStreams = MMap[String, DStream[Row]]()

      val workflowClassName = transaction.getString("transactionName")
      val workflowClass = ApplicationUtils.getWorkflowClass(workflowClassName,
        ApplicationManager.getConfig().tempDir).asInstanceOf[StreamingTransaction]

      val inputDStreams = dStreams.values.toArray
      val sc = inputDStreams(0).context.sparkContext
      workflowClass.preprocess(sc)

      val inputStreams = transaction.getList("inputStreams")
      if (inputStreams != null) {
        val inputStreamsItr = inputStreams.iterator
        while (inputStreamsItr.hasNext) {
          val inputStream = inputStreamsItr.next.asInstanceOf[ConfigObject].toConfig
          val inputStreamName = inputStream.getString("name")
          if (dStreams.contains(inputStreamName)) {
            try {
              logger.info(s"Adding dstream ${inputStreamName} "
                + s"to transformation workflow $workflowClass")
              workflowDStreams += ((inputStreamName,
                dStreams(inputStreamName)))
            } catch {
              case e: Throwable => {
                logger.error("some error", e)
                workflowDStreams += ((inputStreamName,
                  dStreams(inputStreamName)))
              }
            }
          }
        }
      }

      logger.info(s"Calling $workflowClass.process $workflowDStreams")
      val returnDStream =
        workflowClass.processStream(workflowDStreams.toMap, currentWorkflowTime)
      val persistDStreamName = transaction.getString("persistStreamName")
      if (dStreams.contains(persistDStreamName) ) {

        logger.error("Config file is wrong",
          s"${persistDStreamName} returned from $transaction " +
            s"already present in ${dStreams.toString}")
        throw new Exception
      } else {
        dStreams += ((persistDStreamName, returnDStream))
      }
      logger.info("Returned " + returnDStream)
      logger.info("Overall Stream collection " + dStreams)
    }
    dStreams
  }

  /**
   * Invoke persist method on workflow classes to save transformed RDDs
   * @param workflowDStreams
   */
  private def saveDStreams(workflowClassMap: MMap[String, StreamingTransaction],
                           workflowDStreams: MMap[String, DStream[Row]],
                           appConfig: ApplicationConfig)
                          : MMap[String, StreamingTransaction] = {

    val transactionsList = ApplicationManager.getWorkflowConfig.transactions

    val transactionsListItr = transactionsList.iterator
    while (transactionsListItr.hasNext) {
      val transaction = transactionsListItr.next

      val workflowClassName = transaction.getString("transactionName")
      val workflowClass =
        ApplicationUtils.getWorkflowClass(
          workflowClassName,
          ApplicationManager.getConfig().tempDir).asInstanceOf[StreamingTransaction]

      workflowClassMap += ((workflowClassName, workflowClass))
      val persistDStreamName = transaction.getString("persistStreamName")

      if (workflowDStreams.contains(persistDStreamName)
          && transaction.getBoolean("isPersist")) {

        logger.info(s"Adding dstream $persistDStreamName to persist workflow $workflowClass")
        val persistDStream = workflowDStreams(persistDStreamName)
        persistDStream.foreachRDD((rdd, time) => {
          try {
            logger.info(s"persisting RDD of: $persistDStreamName")
            workflowClass.persistStream(rdd, new Time(time.milliseconds))
            DataValidator.printStats()
          } catch {
            case e: Throwable => {

              logger.error("Exception ", e)
              workflowClassMap.foreach {
                case (workflowClassName, workflowClass) => {

                  logger.error("Rolling back transaction",
                    s"$workflowClassName :${workflowClassMap.toString()}")

                  try {
                    workflowClass.rollbackStream(new Time(time.milliseconds))
                  } catch {

                    case ex: Throwable => {
                      logger.error(s"Exception during rollback $workflowClassName :", ex)
                    }
                  }
                }
              }

              logger.error("ERROR", "Stopping Streaming Context.")

              try {
                if (ssc != null) {
                  ssc.stop(true, false)
                }
              } catch {
                case ex: Throwable => {

                  logger.info(s"Consumed following exception because " +
                    s"spark context was NOT stopped gracefully.", ex)
                }
              }
              finally {

                ApplicationManager.synchronized {

                  logger.error("ERROR", "Notifying ApplicationManager to shutdown.")
                  ApplicationManager.stopStreaming = true
                  ApplicationManager.notifyAll()
                }
              }
            }
          }
        })
      }
    }
    workflowClassMap
  }

  /**
   * Add shutdown hook to shut down streaming context gracefully
   */
  private def addShutdownHook(): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        logger.error("#####################Inside shutdown hook#####################", "")

        // When CTRL+C is pressed, we need to reset stopStreaming to TRUE
        // Otherwise depending on the order of Threads getting killed,
        // some thread may try to restart streaming
        ApplicationManager.synchronized {

          logger.error("ERROR", "Notifying ApplicationManager to shutdown.")
          ApplicationManager.stopStreaming = true
          ApplicationManager.notifyAll()
        }

        // stop HTTP server if started
        if( ApplicationManager.getEmbeddedServer != null) {

          logger.info(s"Stopping embedded server")
          ApplicationManager.getEmbeddedServer.stop()
        }

        // stop streaming context and exit gracefully
        // this will ensure that the same files are NOT picked up when job is restarted
        ssc.stop(true, true)
      }
    })
  }

}
