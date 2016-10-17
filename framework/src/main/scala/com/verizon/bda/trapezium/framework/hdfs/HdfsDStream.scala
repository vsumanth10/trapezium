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
package com.verizon.bda.trapezium.framework.hdfs

import com.typesafe.config.{Config, ConfigObject}
import com.verizon.bda.trapezium.framework.ApplicationManager
import com.verizon.bda.trapezium.framework.utils.ApplicationUtils
import com.verizon.bda.trapezium.validation.Validator
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * @author Pankaj on 10/21/15.
 */
private[framework] object HdfsDStream {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Creates streaming context from a HDFS directory
   * Creates streaming DAG
   * @param batchTime
   * @return
   */
  def createStreamingContext(batchTime: Long, checkpointDirectory: String,
                             sparkConf: SparkConf): StreamingContext = {


    // create new streaming context with batch duration and the SparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(batchTime))
    // set checkpoint directory to remove lineage
    ssc.checkpoint(checkpointDirectory)
    ssc
  }

  def createDStreams(ssc: StreamingContext): collection.mutable.Map[String, DStream[Row]] = {

    val appConfig = ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.getWorkflowConfig

    val hdfsConfig = workflowConfig.hdfsStream.asInstanceOf[Config]
    val streamsInfo = hdfsConfig.getList("streamsInfo")
    logger.info(s"STREAM ${streamsInfo.toString}")

    val dStreams = collection.mutable.Map[String, DStream[Row]]()

    val streamsInfoListItr = streamsInfo.iterator
    while (streamsInfoListItr.hasNext) {

      val streamInfo = streamsInfoListItr.next.asInstanceOf[ConfigObject].toConfig
      // create new stream from HDFS location
      val streamDirectory = appConfig.fileSystemPrefix + streamInfo.getConfig("dataDirectory")
        .getString(ApplicationUtils.env)
      val dStreamString = ssc.fileStream[LongWritable, Text, TextInputFormat](
        streamDirectory, (x: Path) => true, newFilesOnly = false)

      logger.info(s"Data Directory: ${streamDirectory}")

      // convert dstream of String into Row
      val dStreamRow = dStreamString.transform((rdd, time) => {

        val rowRDD = rdd.map(line => Row(line._2.toString))

        rowRDD
      })
      val streamName = streamInfo.getString("name")
      val validatedDStream = Validator.getValidatedStream(streamName, dStreamRow, streamInfo)

      dStreams += ((streamName, validatedDStream))
      logger.info(s"Checkpoint Interval ${hdfsConfig.getLong("checkpointInterval")}")
      // checkpoint DStream
      dStreamRow.checkpoint(new Duration(hdfsConfig.getLong("checkpointInterval")))
    }
    dStreams
  }
}
