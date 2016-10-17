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
package com.verizon.bda.trapezium.framework.utils

import java.io.FileNotFoundException
import java.sql.Time

import com.typesafe.config.Config
import com.verizon.bda.trapezium.framework.{ApplicationManager, ApplicationTransaction, ApplicationManagerStartup, StreamingTransaction}
import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.zookeeper.ZooKeeperConnection
import com.verizon.bda.trapezium.framework.{ApplicationManagerStartup, ApplicationTransaction}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, ZooKeeper}
import scala.collection.mutable.{Map => MMap}

import scala.reflect.runtime._
import org.slf4j.LoggerFactory;

/**
  * @author Pankaj on 10/21/15.
  */

private[framework] object ApplicationUtils {

  val logger = LoggerFactory.getLogger(this.getClass)
  val lastSuccessfulAccess: String = "lastSuccessfulAccess"
  val lastSynchronizationTime: String = "lastSynchronizationTime"
  val zkPrefix: String = "/bda/apps/"
  val registerHostName = "driverHostName"

  val env = try {
    val environemnt = scala.io.Source.fromFile("/opt/bda/environment").mkString.trim

    // Hack for Jenkins integration
    if (environemnt == "saiph") {
      "local"
    } else {
      environemnt
    }
  } catch {
    case e: FileNotFoundException => "local"
  }

  def getWorkflowClassList(workflowClassNames: Array[String],
                           tempDir: String): Array[ApplicationTransaction] = {

    val workflowClassList: Array[ApplicationTransaction] =
      new Array[ApplicationTransaction](workflowClassNames.size)

    var index: Int = 0

    workflowClassNames.foreach(workflowClassName => {

      val workflowClass = getWorkflowClass(workflowClassName, tempDir)

      workflowClassList.update(index, workflowClass)
      index = index + 1
    })

    logger.info(s"WorkflowClassList: $workflowClassList")

    workflowClassList

  }

  def getStartupClass(startupClassName: String): ApplicationManagerStartup = {
    // Use reflection in scala to get Singleton instance of workflow classes
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)

    val module = runtimeMirror.staticModule(startupClassName)
    val obj = runtimeMirror.reflectModule(module)
    val workflowClassObj = obj.instance.asInstanceOf[ApplicationManagerStartup]

    workflowClassObj
  }

  def  getWorkflowClass(workflowClassName: String,
                       tempDir: String): ApplicationTransaction = synchronized{

    // Use reflection in scala to get Singleton instance of workflow classes
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(workflowClassName)
    val obj = runtimeMirror.reflectModule(module)
    val workflowClassObj = obj.instance.asInstanceOf[ApplicationTransaction]
    workflowClassObj

  }

  def calculateHdfsRememberDuration: Long = {

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.getWorkflowConfig()

    var minRememberDuration: Long = 0L

    val batchTime = workflowConfig.hdfsStream
      .asInstanceOf[Config].getString("batchTime").toInt

    try {

      val dependentWorkflowTime = getSyncWorkflowWorkflowTime( appConfig, workflowConfig)

      val currentWorkflowTime = getCurrentWorkflowTime( appConfig, workflowConfig )

      logger.info(s"LastSuccessfulAccess for current workflow $currentWorkflowTime")

      if (dependentWorkflowTime != None) {
        logger.info(s"LastSuccessfulAccess for dependent workflow  ${dependentWorkflowTime.get}")


        minRememberDuration =
          (System.currentTimeMillis() - dependentWorkflowTime.get) / 1000
            // commented as per CS-507
            // Math.min(dependentWorkflowTime.get, currentWorkflowTime)) / 1000
      } else {

        minRememberDuration = (System.currentTimeMillis() - currentWorkflowTime) / 1000
      }


    } catch {
      case ex: Throwable => {

        // expected to get exception in local mode
        if ( appConfig.env != "local") {
          logger.error("ZooKeeperException", ex)
        }

      }

    }

    // add buffer
    minRememberDuration + batchTime

  }

  def updateZookeeperValue(path: String,
                           timeStamp: Long,
                           zookeeperList: String): Unit = {

    updateZookeeperValue(path, timeStamp.toString, zookeeperList)

  }

  def updateSynchronizationTime(workflowName: String,
                                timeStamp: Long,
                                zookeeperList: String ): Unit = {

    val key = s"/${workflowName}/${lastSynchronizationTime}"
    updateZookeeperValue(key, timeStamp.toString, zookeeperList)
  }

  def registerHostName(workflowName: String,
                                hostName: String,
                                zookeeperList: String ): Unit = {

    val key = s"/${workflowName}/${registerHostName}"
    updateZookeeperValue(key, hostName, zookeeperList)
  }



  def updateCurrentWorkflowTime(workflowName: String,
                                timeStamp: Long,
                                zookeeperList: String ): Unit = {

    val key = s"/${workflowName}/${lastSuccessfulAccess}"
    updateZookeeperValue(key, timeStamp.toString, zookeeperList)
  }



  def updateZookeeperValue(zkNode: String,
                           value: String,
                           zookeeperList: String): Unit = {
    val zk = ZooKeeperConnection.create(zookeeperList)

    val modified_zkNode = modifyKey(zkNode)

    try {
      checkPath(zk, modified_zkNode)

      logger.info(s"Update $modified_zkNode ${value}")
      zk.setData(modified_zkNode, value.getBytes, -1)

    } catch {

      case ex: Throwable => {
        logger.error("ZooKeeperException", ex)
        throw new Exception(ex)

      }

    }

  }

  def getZookeeperData(appConfig: ApplicationConfig, zkNode: String): Time = {
    var data: Long = 0L

    val zk = ZooKeeperConnection.create(appConfig.zookeeperList)
    val modified_zkNode = modifyKey(zkNode)

    try {

      checkPath(zk, modified_zkNode)

      val zkNodeValue = zk.getData(
        modified_zkNode,
        false,
        null)

      if (zkNodeValue != null) {
        data = new String(zkNodeValue).toLong
      }

    } catch {
      case ex: Throwable => {
        logger.error("ZooKeeperException", ex)
        throw ex
      }
    }

    new Time(data)
  }

  def getSyncWorkflowWorkflowTime(appConfig: ApplicationConfig,
                               workflowConfig: WorkflowConfig): Option[Long] = {

    if (workflowConfig.syncWorkflow != null) {

      val zkNode = s"/${workflowConfig.syncWorkflow}/${lastSynchronizationTime}"
      Some(getLongValFromZk(zkNode, appConfig))
    } else {

      None
    }
  }

  def getCurrentWorkflowTime(appConfig: ApplicationConfig,
                             workflowConfig: WorkflowConfig): Long = {

    val zkNode = s"/${workflowConfig.workflow}/${lastSuccessfulAccess}"
    getLongValFromZk(zkNode, appConfig)
  }

  private def getLongValFromZk(zkNode: String, appConfig: ApplicationConfig): Long = {

    val workflowTimeZk = getValFromZk(zkNode, appConfig.zookeeperList)

    if ( workflowTimeZk != null) {

      workflowTimeZk.toLong
    } else {
      val currentTime = System.currentTimeMillis
      updateZookeeperValue(zkNode, currentTime, appConfig.zookeeperList )
      currentTime
    }

  }

  def getValFromZk(zkNode: String, zookeeperList : String) : String = {
    var valZK: String = null
    val zk = ZooKeeperConnection.create(zookeeperList)

    val modified_zkNode = modifyKey(zkNode)
    logger.info("zk node is " + modified_zkNode)

    try {
      checkPath(zk, modified_zkNode)

      val valueZK = zk.getData(
        modified_zkNode,
        false,
        null)

      if (valueZK != null) {
        valZK = new String(valueZK)
      }

    } catch {
      case ex: Throwable => {
        logger.error("ZooKeeperException", ex)
         throw ex
      }
    }
    valZK

  }

  def deleteKeyZk(key: String, zookeeperList : String): Unit = {
    val zk = ZooKeeperConnection.create(zookeeperList)
    try {
      val keyModified = modifyKey(key)
      logger.info(keyModified)
      zk.delete(keyModified, zk.exists(keyModified, true).getVersion())
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        logger.error("ZooKeeperException", ex)
      }
    }
  }


  def checkPath(zk: ZooKeeper, path: String): Unit = {

    if (zk.exists(path, false) == null) {
      logger.info(s"zkNode $path does not exist. Let's create it.")

      synchronized {
        val zkNodes = path.substring(1).split("/");
        logger.info(s"${path}")
        var currPath = ""
        zkNodes.foreach(zkNode => {
          currPath += s"/$zkNode"
          logger.info(s"Checking ZK Path $currPath")

          if (null == zk.exists(currPath, false)) {
            logger.info(s"Create ${currPath}")

            zk.create(currPath,
              null,
              Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT)

          }
        })
      }
    }
  }

  def modifyKey(key: String): String = {

    s"${zkPrefix}${ApplicationManager.getConfig().persistSchema}${key}"

  }

  def getCurrentWorkflowKafkaPath(appConfig: ApplicationConfig,
                                  workflowConfig: WorkflowConfig): String = {

    buildKafkaZkPath( appConfig, workflowConfig, workflowConfig.workflow)
  }

  def getDependentWorkflowKafkaPath(appConfig: ApplicationConfig,
                                    workflowConfig: WorkflowConfig): Option[String] = {

    if (workflowConfig.syncWorkflow != null) {

      Option(buildKafkaZkPath(appConfig, workflowConfig, workflowConfig.syncWorkflow))
    } else {

      None
    }
  }

  def buildKafkaZkPath(appConfig: ApplicationConfig,
                       workflowConfig: WorkflowConfig,
                       workflowName: String): String = {

    val kafkaConfig = workflowConfig.kafkaTopicInfo.asInstanceOf[Config]
    val streamsInfo = kafkaConfig.getConfigList("streamsInfo")

    val streamInfo = streamsInfo.get(0)
    val topicName = streamInfo.getString("topicName")

    val zkpath = new StringBuilder(ApplicationUtils.zkPrefix).append(appConfig.persistSchema)
      .append("/").append(workflowName)
      .append("/").append(topicName).toString()

    logger.info(s"ZooKeeper Path:$zkpath")

    zkpath

  }

  def getDependentsWorkflowTime(appConfig: ApplicationConfig,
                                workflowConfig: WorkflowConfig): MMap[String, Option[Long]] = {
    val dependentWorkFlowTime = MMap[String, Option[Long]]()
    val dependentWorkflows = workflowConfig.dependentWorkflows
    if (dependentWorkflows != null) {
      val dependentItr = dependentWorkflows.iterator()
      while (dependentItr.hasNext){
        val dependentWorkflow = dependentItr.next()
        val zkNode = s"/${dependentWorkflow}/${lastSuccessfulAccess}"
        logger.info("zkNode")
        dependentWorkFlowTime += ((dependentWorkflow, Some(getLongValFromZk(zkNode, appConfig))))
      }
    }
    dependentWorkFlowTime
  }

  def isDependentWorkflowExecuted (appConfig: ApplicationConfig,
                                   workflowConfig: WorkflowConfig): Boolean = {

    val dependentWorkflows = workflowConfig.dependentWorkflows
    if (dependentWorkflows != null) {
    val dependentsWorkflowTime = getDependentsWorkflowTime(appConfig, workflowConfig)
      val currentWorkflowTime = ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig)
      val dependentItr = dependentWorkflows.iterator()
        while (dependentItr.hasNext){
          val dependentWFName = dependentItr.next()
          if (currentWorkflowTime >= dependentsWorkflowTime(dependentWFName).get) return false
        }
        true
    } else true
  }

  def waitForDependentWorkflow (appConfig: ApplicationConfig,
                         workflowConfig: WorkflowConfig): Boolean = {
    while (true){
      val isDependentWfExecuted = isDependentWorkflowExecuted(appConfig, workflowConfig)

      if (isDependentWfExecuted) return true
      logger.info("Waiting for DependentWorkflow to be executed" +
        " ..isDependentWorkflowExecuted " + isDependentWfExecuted)
       Thread.sleep(workflowConfig.dependentFrequencyToCheck * 1000L)
    }
    false

  }

}
