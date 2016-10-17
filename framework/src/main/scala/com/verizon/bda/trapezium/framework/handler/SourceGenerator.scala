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

import java.io._
import java.util.regex.Pattern
import com.typesafe.config.{ConfigFactory, Config, ConfigObject}
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.manager.{WorkflowConfig, ApplicationConfig}
import com.verizon.bda.trapezium.framework.utils.{ScanFS, ApplicationUtils}
import com.verizon.bda.trapezium.validation.{DataValidator, ValidationConfig}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{Map => MMap}
import java.sql.Time
import java.text.SimpleDateFormat
import java.util.{GregorianCalendar, Calendar, Date}



// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on

/**
 * @author sumanth.venkatasubbaiah Utility to construct RDD's from files
 *         debasish83 Added SourceGenerator which constructs iterator of time and RDD to support
 *                    reading FS by timeStamp, fileName, reading DAO for model transaction and
 *                    read kafka to get the batch data based on custom processing logic
 *        Hutashan Added file split
 */
private[framework] abstract class SourceGenerator(workflowConfig: WorkflowConfig,
                                                  appConfig: ApplicationConfig,
                                                  sc: SparkContext)

  extends Iterator[(Time, Map[String, DataFrame], String)] {
  val inputSources = scala.collection.mutable.Set[String]()
  var mode = ""
  // Get all the input source name
  val transactionList = workflowConfig.transactions
  transactionList.asScala.foreach { transaction: Config =>
    val txnInput = transaction.getList("inputData")
    txnInput.asScala.foreach { source =>
      val inputSource = source.asInstanceOf[ConfigObject].toConfig
      val inputName = inputSource.getString("name")
      inputSources += inputName

    }
  }
  // end txn list

  val hdfsBatchConfig = workflowConfig.hdfsFileBatch.asInstanceOf[Config]
  val batchInfoList = hdfsBatchConfig.getList("batchInfo")
  if (batchInfoList(0).asInstanceOf[ConfigObject].toConfig.hasPath("splitfile")){
    mode = "splitFile"
  }
}

object SourceGenerator {

  val logger = LoggerFactory.getLogger(this.getClass)

  def validateRDD(rdd: RDD[Row],
                  batchData: Config): DataFrame = {

    val appConfig = ApplicationManager.getConfig()
    val workFlowConfig = ApplicationManager.getWorkflowConfig
    val inputName = batchData.getString("name")

    val validationConfig =
      ValidationConfig.getValidationConfig(
        appConfig, workFlowConfig, inputName)

    val validator = DataValidator(rdd.sparkContext, inputName)
    validator.validateDf(rdd, validationConfig)
  }

  def getFileFormat(batchData: Config): String = {
    try {
      batchData.getString("fileFormat")
    } catch {
      case ex: Throwable => {
        logger.warn(s"No file format present. Using default text")
        "text"
      }
    }
  }
}
 object DynamicSchemaHelper {

   import org.apache.spark.rdd.RDD
   val logger = LoggerFactory.getLogger(this.getClass)

   def generateDataFrame(csv: RDD[Row],
                        name: String,
                        file: String,
                        sc: SparkContext): (String, DataFrame) = {
    val filename = new File(file).getName()
    var config: Config = null

    var conffilename: String = null
    var conffilecontents: String = null
    logger.info("Input file path:" + file)
    if (file.startsWith("file:")) {
      conffilename = file.stripPrefix("file:") + ".conf"
      logger.info("File path" + conffilename)
      config = ConfigFactory.parseFile(new File(conffilename))
      conffilecontents = FileUtils.readFileToString(new File(conffilename))
    }
    else
    {

      conffilename = file + ".conf"
      val pt: Path = new Path(conffilename)
      var fs: FileSystem = null
      var br: BufferedReader = null
      try {
        fs = FileSystem.get(new Configuration())
        br = new BufferedReader(new InputStreamReader(fs.open(pt)))
        config = ConfigFactory.parseReader(br)
        val writer: Writer = new StringWriter()
        IOUtils.copy(fs.open(pt), writer, "UTF-8")
        conffilecontents = writer.toString
      }
      finally
      {
        if (br != null)
           {
             br.close()
           }
       if (fs != null)
           {
             fs.close()
           }
      }

    }
    val validator = DataValidator(csv.sparkContext, name)
    logger.info(config.toString)
    logger.info("Config file path:" + conffilename)
    (conffilecontents,
      validator.validateDf(csv, config.getConfig("validation")))
  }

}

private[framework] case class SeqSourceGenerator(workflowConfig: WorkflowConfig,
                                                 appConfig: ApplicationConfig,
                                                 sc: SparkContext,
                                                 batches: Seq[Map[String, DataFrame]])
  extends SourceGenerator(workflowConfig, appConfig, sc) {
  var index = 0
    mode = "Seq"
  def next(): (Time, Map[String, DataFrame], String) = {
    val workflowTime = new Time(System.currentTimeMillis)
    val dfs = batchInfoList.asScala.map { batchConfig =>
      val batchData = batchConfig.asInstanceOf[ConfigObject].toConfig
      val name = batchData.getString("name")
      name -> batches(index)(name)
    }.toMap

    index += 1
    (workflowTime, dfs, mode)
  }

  def hasNext(): Boolean = {
    if (index < batches.size) return true
    else return false
  }
}

/**
 *
 * @param workflowConfig the workflow for which RDD's needs to be created
 * @param appConfig Application config
 * @param sc Spark Context instance
 * @param maxIterations -1 denotes infinite iterations, positive numbers are bounds for batch runs
 */
// TO DO :
private[framework] case class
FileSourceGenerator(workflowConfig: WorkflowConfig,
                    appConfig: ApplicationConfig,
                    sc: SparkContext,
                    var maxIterations: Long = -1)  // oneTime is true then run only 1 times
  extends SourceGenerator(workflowConfig, appConfig, sc) {
  var iterations = 0L

  import FileSourceGenerator.getSortedFileMap
  import FileSourceGenerator.getWorkFlowTime
  var mapFile: java.util.TreeMap[Date, StringBuilder] = null
  var iteratorMap: Iterator[(Date, StringBuilder)] = Iterator.empty
  import FileSourceGenerator.addDF

  val logger = LoggerFactory.getLogger(this.getClass)

      /**
        *
        * @return Map of (String, RDD[Row]) and associated workflowTime for the data
        */
      def next(): (Time, Map[String, DataFrame], String) = {
        var workflowTime = new Time(System.currentTimeMillis)
        var dataMap = MMap[String, DataFrame]()
        batchInfoList.asScala.foreach { batchConfig =>
          val batchData = batchConfig.asInstanceOf[ConfigObject].toConfig
          val name = batchData.getString("name")
          val dataDirectoryConfig = batchData.getConfig("dataDirectory")
          val dataDir = {
            if (dataDirectoryConfig.hasPath("query")) {
              appConfig.fileSystemPrefix + dataDirectoryConfig
                .getString(ApplicationUtils.env) + dataDirectoryConfig.getString("query")
            } else {
              appConfig.fileSystemPrefix + dataDirectoryConfig
                .getString(ApplicationUtils.env)
            }
          }
          val oneTime = batchData.getString("oneTime").toBoolean
          val currentWorkflowTime = {
            if (!oneTime) {
              ApplicationUtils.getCurrentWorkflowTime(appConfig, workflowConfig)
            } else {
              if (maxIterations == -1) maxIterations = 1 // onlyDir is true then run only 1 times
              -1L
            }
          }
          if (!batchData.hasPath("splitfile")) {

            val batchFiles = ScanFS.getFiles(dataDir, currentWorkflowTime)
            logger.info(s"Number of available files for processing for source $name = " +
              s": ${batchFiles.size}")
            if (batchFiles.size > 0) {
              batchFiles.mkString(",")
              logger.debug(s"list of files for this run " +  batchFiles.mkString(","))
              addDF(sc, batchFiles, batchData, dataMap)
            }
            iterations += 1
          } else {
         //   maxIterations = 1
            mode = "splitFile"
            val splitfile = batchData.getConfig("splitfile")

            val workSpaceDataDirectory = batchData.getConfig(
              "dataDirectory").getString(ApplicationUtils.env)
            if (mapFile == null) {
              iterations += 1
              val batchFiles = ScanFS.getFiles(dataDir, currentWorkflowTime)
              logger.info(s"Number of available files for processing for source $name = " +
                s": ${batchFiles.size}")
              if (batchFiles.size > 0) {
                mapFile = getSortedFileMap(
                  batchFiles.toList, splitfile,
                  workSpaceDataDirectory)
                iteratorMap = mapFile.iterator
              }
            }
            if (iteratorMap.hasNext) {
              val (sDate, files) = iteratorMap.next()
              logger.debug("fileSplit running for date " + sDate + " , files are " +
                files.toString())
              workflowTime = getWorkFlowTime(sDate, currentWorkflowTime)
              addDF(sc, files.toString().split(","), batchData, dataMap)
            }
          }
        }
          (workflowTime, dataMap.toMap, mode)
        }
        def hasNext(): Boolean = {
          if (!mode.equals("splitFile")) {
            if (maxIterations < 0) return true
            if (iterations < maxIterations) return true
          } else {
            if (maxIterations < 0 &&  mapFile == null) return true
            if (iterations < maxIterations &&  mapFile == null) return true
             if (iteratorMap.hasNext) return true
            else {
               logger.info("reset counter because of filesplit")
               mapFile = null
               return false
             }

          }
          return false
        }

      }

object FileSourceGenerator {
  import DynamicSchemaHelper.generateDataFrame

  val logger = LoggerFactory.getLogger(this.getClass)
  def addDF(sc: SparkContext,
            input: Array[String],
            batchData: Config,
            dataMap: MMap[String, DataFrame]): MMap[String, DataFrame] = {
    val name = batchData.getString("name")
    SourceGenerator.getFileFormat(batchData).toUpperCase match {
      case "PARQUET" => {
        logger.info(s"input source is Parquet")
        dataMap += ((name, SQLContext.getOrCreate(sc).read.parquet(input: _*)))
      }
      case "AVRO" => {
        logger.info(s"input source is Avro")
        dataMap += ((name, SQLContext.getOrCreate(sc).read.format("com.databricks.spark.avro")
          .load(input: _*)))
      }
      case "DYNAMICSCHEMA" => {
        val configfiles = input.filter(filename => filename.endsWith(".conf"))
        input.filter(filename => !filename.endsWith(".conf")).map((file : String) =>
        {
          logger.info("FileName" + file)
          dataMap +=
            (generateDataFrame(sc.textFile(Array(file).mkString(",")).
              map(line => Row(line.toString)), name, file, sc))
        }
        )
        dataMap
      }
      case "JSON" => {
        logger.info(s"input source is Json")
        dataMap += ((name, SQLContext.getOrCreate(sc).read.text(input: _*)))
      }
      case _ => {
        val rdd = sc.textFile(input.mkString(",")).map(line => Row(line.toString))
        dataMap += ((name, SourceGenerator.validateRDD(rdd, batchData)))

      }
    }
  }

/**
          * This method invoked only once in each cycle if filesplit is required.
          * Sorted files according to date
  */

        def getSortedFileMap(inputFileList: java.util.List[String],
                             splitfile: Config,
                             workSpaceDataDirectory : String
                            ): java.util.TreeMap[Date, StringBuilder] = {

          val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat(
            splitfile.getString("dateformat"))
          val mapFile = new java.util.TreeMap[Date, StringBuilder]()
         if (!splitfile.hasPath("regexFile")) {
           val startDateIndex = splitfile.getString("startDateIndex").toInt
           val endDateIndex = splitfile.getString("endDateIndex").toInt
           val workSpaceLength = {
             if (inputFileList(0) != null) {
               inputFileList(0).indexOf(workSpaceDataDirectory) + workSpaceDataDirectory.length
             } else {
               logger.info("length count == 0")
               0
             }
           }
           inputFileList.foreach { file =>
             try {
               val fileLen = file.length
               val fileDate = file.substring(
                 workSpaceLength + startDateIndex, workSpaceLength + endDateIndex)
               val dt: Date = simpleDateFormat.parse(fileDate)
               val fileName = mapFile.get(dt)
               if (fileName == null) {
                 mapFile.put(dt, new StringBuilder(file))
               } else {
                 fileName.append(",")
                 fileName.append(file)
                 mapFile.put(dt, fileName)
               }
             }
             catch {
               case e: Throwable =>
                 logger.error(s"Error in file parsing. File name $file ", e)
             }
           }
         } else {

           val pattern = Pattern.compile(splitfile.getString("regexFile"))
           inputFileList.foreach { file =>
             try {
               val fileDate = {
              val matcher = pattern.matcher(file)
               if (matcher.find()){
                 matcher.group()
               } else ""
               }
               val dt: Date = simpleDateFormat.parse(fileDate)
               val fileName = mapFile.get(dt)
               if (fileName == null) {
                 mapFile.put(dt, new StringBuilder(file))
               } else {
                 fileName.append(",")
                 fileName.append(file)
                 mapFile.put(dt, fileName)
               }
             }
             catch {
               case e: Throwable =>
                 logger.error(s"Error in file parsing. File name $file ", e)
             }
           }

         }

          mapFile
        }

        // TO DO
        // Need to handle some corner case because it may create some performance issue in regular.
        // this part will impact timeseries flow. I will check with team


        def getWorkFlowTime(dt: Date, lastSuccessfulBatchComplete: Long): Time = {
          val todayDate = new Date(System.currentTimeMillis())
          if (dt.before(todayDate)) (new Time(dt.getTime))
          else new Time(System.currentTimeMillis())
        }


      }
// 1. Add DAO support to read Hive and Cassandra for ModelTransaction
// 2. Add Kafka support to read the online stream data that's added to FS.
// Is the time series maintained in Kafka batch path ?
