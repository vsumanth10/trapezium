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
import java.util.{Date, Calendar, GregorianCalendar}
import com.verizon.bda.trapezium.framework.utils.{ApplicationUtils, ScanFS}
import com.verizon.bda.trapezium.validation.{ValidationPreparer, ValidationConfig}
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on
/**
  * @author hutashan
  *         tests for source generators
  */
class SourceGeneratorFileSplitSuite extends FunSuite with LocalSparkContext with BeforeAndAfter {

  val logger = LoggerFactory.getLogger(this.getClass)

  before {
    FileCopy.fileDelete
    FileCopy.copyFiles(2)

  }
  override def afterAll(): Unit = {
    FileCopy.fileDelete
    super.afterAll()
  }

  test("create dir"){

    val dataDir = "src/test/data/hdfs/filesplit/"
    val splitConf = ValidationPreparer.getConfigFileSplit("fileSplitWorkFlow")
    val batchFiles = ScanFS.getFiles(dataDir, System.currentTimeMillis()-5000)
    assert(batchFiles.size > 0 )
    val  mapFile = FileSourceGenerator.getSortedFileMap(
      batchFiles.toList, splitConf, dataDir )
      assert(mapFile.size() == 2)

    val splitConf1 = ValidationPreparer.getConfigFileSplit("fileSplitRegexWorkFlow")
    val  mapFile1 = FileSourceGenerator.getSortedFileMap(
      batchFiles.toList, splitConf1, dataDir )
    assert(mapFile1.size() == 2)
  }

test ("workflowtime") {
  val calendar = new GregorianCalendar()
  // Move calendar to yesterday
   calendar.add(Calendar.DATE, -1)
  val dt = getStartOfDay(new Date(calendar.getTime.getTime))
  logger.info("yesterdaydate   " + dt + " millisecond " + dt.getTime)
  val time1 = FileSourceGenerator.getWorkFlowTime(dt, 10000L)
  logger.info("result for yesterday date   "
    + new Date(time1.getTime).toString + "millsecond" + time1.getTime)
  // Move calendar to yesterday
  assert(time1.getTime <= dt.getTime )
  val dt1 = new Date(System.currentTimeMillis()-2000)
  logger.info("dt   " + dt1.getTime)
  val time = FileSourceGenerator.getWorkFlowTime(dt1, 10000L)
  logger.info("today date   " + System.currentTimeMillis() + "result date  " + time.getTime)
  assert(dt1.getTime==time.getTime)
}

  def getStartOfDay(dt : java.util.Date) : java.util.Date = {
    val calendar = Calendar.getInstance()
    calendar.setTime(dt)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    return calendar.getTime()
  }
}
