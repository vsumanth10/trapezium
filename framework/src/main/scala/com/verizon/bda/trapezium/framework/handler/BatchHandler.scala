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
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Timer, TimerTask}
import com.typesafe.config.{Config, ConfigList, ConfigObject}
import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.framework.{ApplicationManager, BatchTransaction}
import com.verizon.bda.trapezium.validation.DataValidator
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{Map => MMap}
import com.verizon.bda.trapezium.framework.utils.Waiter
import org.slf4j.LoggerFactory;

/**
 * @author sumanth.venkatasubbaiah Handles the batch workflows
 *         debasish83: integrated timer scheduling API for local and integration test
  *        hutashan: Added accumlator for batch stats
 *
 */
private[framework] class BatchHandler(val workFlowConfig : WorkflowConfig,
                                      val appConfig: ApplicationConfig,
                                      val batches : SourceGenerator)
                                     (implicit val sc: SparkContext) extends TimerTask with Waiter {
  /**
   * method called by the TimerTask when a run is scheduled.
   */
  val simpleFormat = new SimpleDateFormat("yyyy-MM-dd")
  val logger = LoggerFactory.getLogger(this.getClass)

  override def run(): Unit = {
    try {
      logger.info("workflow name" + workFlowConfig.workflow)
      ApplicationManager.setWorkflowConfig(workFlowConfig.workflow)
      val batchMode = workFlowConfig.dataSource
      logger.info("mode == " + batches.mode )
      batchMode match {
        case "HDFS" => {
          logger.info(s"Running BatchHandler in mode: $batchMode")
          handleWorkFlow
          logger.info(s"batch completed for this run.")
          }
          case _ => {
            logger.error("Not implemented mode. Exiting.. ", batchMode)
            notifyStop()
          }
        }
    } catch {
      case e: Throwable =>
        logger.error(s"Error in running the workflow", e)
        notifyError(e)
    }
  }
  def handleWorkFlow : Unit = {
    if (batches.hasNext) {
      ApplicationUtils.waitForDependentWorkflow(appConfig, workFlowConfig)
      val (workflowTimeToSave, runSuccess, batchMode) = executeWorkFlow
      logger.info("going to call complete method")
      if (batchMode.equals("splitFile")) {
        completed(new Time(System.currentTimeMillis()), runSuccess)
        val keyFileProcessed = s"/etl/${workFlowConfig.workflow}/fileProccessed"
        logger.info("Deleting key fileProccessed")
        ApplicationUtils.deleteKeyZk(keyFileProcessed, appConfig.zookeeperList)
      }
      else completed(workflowTimeToSave, runSuccess)
    } else {
      logger.info("Could not generate RDD in this batch run.")
      notifyStop()
    }
  }


  def executeWorkFlow() : (Time, Boolean, String) = {
    var runSuccess = false


    var workflowTimeToSave = new Time(System.currentTimeMillis)
    if (batches.hasNext) {

      if (batches.mode.equals("splitFile")) {
        while (batches.hasNext) {

          val (workflowTime, rddMap, mode) = batches.next()


          workflowTimeToSave = workflowTime
            val LastCal = Calendar.getInstance()
            val currentCal = Calendar.getInstance()
            val keyFileProcessed = s"/etl/${workFlowConfig.workflow}/fileProccessed"
            val lastProcessedDateFiles = ApplicationUtils.getValFromZk(keyFileProcessed,
              appConfig.zookeeperList)
            if (lastProcessedDateFiles != null) {
              val lastProcessedDateFilesStr = new String(lastProcessedDateFiles).toLong
              logger.info("Test processed time " + new Date(lastProcessedDateFilesStr))
              LastCal.setTime(new Date(lastProcessedDateFilesStr))
              currentCal.setTime(workflowTime)
              if (LastCal.before(currentCal)) {
                logger.info(s"Starting Batch transaction with workflowTime: $workflowTime")
                if (rddMap.size > 0) {
                  runSuccess = processAndPersist(workflowTime, rddMap)
                }
                else logger.warn("No new RDD in this batch run.")
              } else {
                logger.debug("Skipping files")
              }
            } else {
              logger.info(s"Starting Batch transaction with workflowTime: $workflowTime")
              logger.info(s"file split mode first time")
              if (rddMap.size > 0) {
                runSuccess = processAndPersist(workflowTime, rddMap)
              }
              else logger.warn("No new RDD in this batch run.")
            }
            val processedTime: Long = workflowTime.getTime
            logger.info("setting date " + new Date(processedTime.toLong))
            ApplicationUtils.updateZookeeperValue(keyFileProcessed,
              workflowTime.getTime, appConfig.zookeeperList)
        }
      } else {
        val (wkTime, rddbatch, modeFromFirstRun) = batches.next()
        if (rddbatch.size > 0) {
          logger.info("files from batch time")
          runSuccess = processAndPersist(wkTime, rddbatch)
        }
        else logger.warn("No new RDD in this batch run.")
      }
    }
    (workflowTimeToSave, runSuccess, batches.mode)
  }



  def processAndPersist(workflowTime: Time,
                        dfMap: Map[String,
                          DataFrame]): Boolean = {
    try {
      val processedDataMap = processTransactions(workflowTime, dfMap)
      saveData(workflowTime, processedDataMap )
    } catch {
      case e: Throwable => {
        logger.error(s"Caught Exception during processing", e)
        rollBackTransactions(workflowTime)
       throw e

      }
    }
  }

  /**
   * method used to call the process method of the transactions in the current workflow
   * @param dataMap
   * @return map of string and DataFrame
   */
  private def processTransactions(workflowTime: Time,
                                  dataMap: Map[String, DataFrame]): MMap[String, DataFrame] = {
    val transactionList = workFlowConfig.transactions
    var allTransactionInputData = MMap[String, DataFrame]()
    transactionList.asScala.foreach { transaction: Config => {

      var transactionInputData = MMap[String, DataFrame]()
      val transactionClassName = transaction.getString("transactionName")
      val transactionClass: BatchTransaction = ApplicationUtils.getWorkflowClass(
        transactionClassName, appConfig.tempDir).asInstanceOf[BatchTransaction]

      transactionClass.preprocess(sc)

      // MMap to hold the list of all input data sets for this transaction
      val inputData: ConfigList = transaction.getList("inputData")
      inputData.asScala.foreach { s =>
        val inputSource = s.asInstanceOf[ConfigObject].toConfig
        val inputName = inputSource.getString("name")

        if (dataMap.keys.filter(name => name.contains(inputName)).size > 0) {
          dataMap.keys.filter(name => name.contains(inputName)).map (name => {
            logger.info(s"Adding RDD ${name} to transaction " +
              s"workflow ${transactionClassName}")
            transactionInputData += ((name, dataMap(name)))
          }
          )
        } else if ( allTransactionInputData.contains(inputName)) {
          // DF is output from an earlier transaction
          logger.info(s"Adding RDD ${inputName} to transaction " +
            s"workflow ${transactionClassName}")
          transactionInputData += ((inputName, allTransactionInputData(inputName)))
        }
      }

      logger.info(s"Calling $transactionClass.process with " +
        s"input $transactionInputData")
      val processedData = transactionClass.processBatch( transactionInputData.toMap, workflowTime)

      val persistData = transaction.getString("persistDataName")

      if (transactionInputData.contains(persistData)) {

        logger.error("Config file is wrong",
          s"${persistData} returned from $transaction " +
            s"already present in ${dataMap.toString}")
        throw new Exception
      } else {
        transactionInputData += ((persistData, processedData))
      }

      allTransactionInputData ++= transactionInputData
    } // end list of transactions
      logger.info("Overall RDD collection " + dataMap)
    }

    allTransactionInputData
  }

  /**
    * method to update timestamp in zookeeper
    * This method is called only if all the transactions that are part of the workflow
    * are complted successfully.
    */
  private def completed(workflowTime: Time, isSuccess: Boolean): Unit = {
    try {
      if (isSuccess) {
        logger.info(s"Workflow with time $workflowTime completed successfully.")
        ApplicationManager.updateWorkflowTime( workflowTime.getTime, workFlowConfig.workflow)
      } else {
        logger.info("process method is returning false")
      }
    } catch {
      case e: Throwable => {
        logger.error(s"Caught Exception during processing", e)
        rollBackTransactions(workflowTime)
        throw e
      }
    }

  }

  /**
   * method to roll back all the transactions
   * All the transactions in the workflow will be rolled back if any error/exception occurs
   * during the work-flow execution.
   */
  private def rollBackTransactions(workflowTime: Time): Unit = {

    logger.warn(s"Rolling back all transactions in the current workFlow")

    val transactionList = workFlowConfig.transactions
    transactionList.asScala.foreach { transaction: Config =>
      val transactionClassName = transaction.getString("transactionName")
      val transactionClass: BatchTransaction =
        ApplicationUtils.getWorkflowClass(
          transactionClassName,
          appConfig.tempDir).asInstanceOf[BatchTransaction]
      try {
        logger.info(s"Rolling back transaction: $transactionClassName with batchTime:" +
          s" ${workflowTime}")
        transactionClass.rollbackBatch(workflowTime)
      } catch {
        case e: Throwable =>
          logger.error(s"Exception caught while rollback of " +
            s"transaction $transactionClassName with batchTime: ${workflowTime}", e)
          throw e
      }
    }
  }

  /**
   *
   * @param dataMap map of DataFrames which contains the dataframes that will be persisted.
   * @return true if persist is successful, false otherwise
   */
  private def saveData(workflowTime: Time,
                       dataMap: MMap[String, DataFrame]): Boolean = {
    val transactionList = workFlowConfig.transactions
    transactionList.asScala.foreach { transaction: Config => {
      val transactionClassName = transaction.getString("transactionName")
      val transactionClass: BatchTransaction = ApplicationUtils.getWorkflowClass(
        transactionClassName, appConfig.tempDir).asInstanceOf[BatchTransaction]

      val persistDataName = transaction.getString("persistDataName")
      val isPersist = {
        if (transaction.hasPath("isPersist")){
          transaction.getBoolean("isPersist")
        } else {
          true
        }
      }

      if (dataMap.contains(persistDataName) && isPersist) {
        try {
          val persistDF = dataMap(persistDataName)
            logger.info(s"Persisting data: ${persistDataName}")
            transactionClass.persistBatch(persistDF, workflowTime)
          DataValidator.printStats()
        } catch {
          case e: Throwable => {
            logger.error(s"Exception occured while running transaction: $transactionClassName", e)
            throw e
          }
        }
      } // end if
    }
    } // end transaction list iteration
    true
  } // end saveData
}

private[framework] object BatchHandler {

  val logger = LoggerFactory.getLogger(this.getClass)
  /**
   *
   * Schedules the batch workflows
   * @param workFlowConfig the workflow config
   * @param sources source generators for seq, files and kafka
   * @param appConfig the ApplicationConfig object
   * @param sc SparkContext instance
   */
  def scheduleBatchRun(workFlowConfig: WorkflowConfig,
                       sources: SourceGenerator,
                       appConfig: ApplicationConfig,
                       sc: SparkContext): Unit = {
    val timer: Timer = new Timer(true)
    try {

      logger.info("Starting Batch WorkFlow")
      val batchTime: Long =
        workFlowConfig
          .hdfsFileBatch
          .asInstanceOf[Config]
          .getLong("batchTime")

      if (batchTime <= 0) {
        logger.error("Invalid batchTime.", "batchTime has to be greater than 0 seconds")
        System.exit(-1)
      }
      val nextRun = workFlowConfig.hdfsFileBatch.asInstanceOf[Config].getLong("timerStartDelay")
      val batchHandler = new BatchHandler(workFlowConfig, appConfig, sources)(sc)
      timer.scheduleAtFixedRate(batchHandler, nextRun, batchTime * 1000)
      if(batchHandler.waitForStopOrError) stopTimer(timer)
    } catch {
      case e: Throwable =>
        logger.error("Caught exception during scheduling BatchJob", e)
        stopTimer(timer)
        throw e
    }
  }

  def stopTimer(timer: Timer): Unit = {
    timer.cancel()
    timer.purge()
  }
}
