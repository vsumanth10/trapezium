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
package com.verizon.bda.trapezium.validation

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.typesafe.config.{ConfigObject, Config}
import com.verizon.bda.trapezium.framework.ApplicationManager
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.TestSuiteBase
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import scala.io.Source

class ValidatorSuite extends FunSuite with TestSuiteBase {

  val logger = LoggerFactory.getLogger(this.getClass)

  val badRecordCount = 6L
  // tested for checkSpace,minLength,columnCheckFails,maxLength,checklongfail,checkIntFail

  val path = "src/test/data/hdfs/source3/file3.csv"
  val path1 = "src/test/data/hdfs/source4"

  test("Test startValidation : End to end validation module testing") {
    val rddRow = sc.textFile(path).map(x => Row(x))
    val recordCount = rddRow.count() - 1 // skip header
    val validatorConfig = ValidationPreparer.getValidationConfig()
    val df = Validator.startValidation("test1", rddRow, validatorConfig)
    val dfCount = df.count()
    val column = validatorConfig.getStringList(Constants.Column)
    val expectedNumberOfcolumn = column.size()
    assert(dfCount == recordCount - badRecordCount)
    assert(df.columns(df.columns.length - 1) == column.get(expectedNumberOfcolumn - 1))
    assert(df.columns.length == expectedNumberOfcolumn)
    assert(rddRow.count() == Validator.countTotal("test1"))
    assert(dfCount == Validator.countFiltered("test1"))
  }

  test("Test startValidation : timestamp DF") {
    val rddRow = sc.textFile(path1).map(x => Row(x))
    val recordCount = rddRow.count() - 1 // skip header
    val validatorConfig = ValidationPreparer.getValidationConfigSources2()
    val df = Validator.startValidation("test2", rddRow, validatorConfig)
    val dfCount = df.count()
    val column = validatorConfig.getStringList(Constants.Column)
    val expectedNumberOfcolumn = column.size()
    assert(df.columns(df.columns.length - 1) == column.get(expectedNumberOfcolumn - 1))
    val data = df.collect()
    val sFormat: SimpleDateFormat = new SimpleDateFormat(
      validatorConfig.getString(Constants.DateFormat))
    assert(data(0).getTimestamp(3).equals(
      new Timestamp(sFormat.parse("2015-07-07 01:37:08").getTime)))
  }

  test("printBatchStats test") {
    val rddRow = sc.textFile(path1).map(x => Row(x))
    val validatorConfig = ValidationPreparer.getValidationConfigSources2()
    val df = Validator.startValidation("test3", rddRow, validatorConfig)
    val dfCount = df.count()
    assert(rddRow.count() == Validator.countTotal("test3"))
    assert(dfCount == Validator.countFiltered("test3"))
  }

  def testValidateStreams(inputStream: DStream[Row]): DStream[Row] = {
    ApplicationManager.getConfig()
    val workflowConfig = ApplicationManager.setWorkflowConfig("streamValidationWorkFlow")

    val hdfsConfig = workflowConfig.hdfsStream.asInstanceOf[Config]
    val streamsInfo = hdfsConfig.getList("streamsInfo").get(0).asInstanceOf[ConfigObject].toConfig

    val dataValidated = Validator.getValidatedStream("test", inputStream, streamsInfo)
    dataValidated
  }

  test("Test validationStream") {
    val input1: Seq[Row] = Source.fromFile(path).mkString("").split("\n").toSeq.map(l => Row(l))
    val inputStream = Seq(input1, input1, input1)
    val ssc = setupStreams(inputStream, testValidateStreams)
    val output: Seq[Seq[String]] = runStreams[String](ssc, 3, 3)
    output.foreach(s => logger.info(s"${s.size} -> ${s.toString}"))

    assert(output.size == 3)
    assert(output(0).size == 97)
    assert(output(1).size == 97)
    assert(output(2).size == 97)
    assert(Validator.countFiltered("test") == 291)
  }
}
