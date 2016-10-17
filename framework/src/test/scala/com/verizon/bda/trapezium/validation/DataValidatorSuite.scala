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

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

class DataValidatorSuite extends TestSuiteBase {

  val logger = LoggerFactory.getLogger(this.getClass)

  val badRecordCount = 6L
  val path = "src/test/data/hdfs/source3"
  val validatorConfig = ValidationPreparer.getValidationConfig()
  var parser : CSVParser = null

  def setParser(delimter : Char) : Unit = {
    parser = new CSVParser(delimter)
  }

  test("Test validate") {
    val rddRow = sc.textFile(path).map(x => Row(x))
    val recordCount = rddRow.count() - 1 // skip header
    val df = DataValidator(sc).validateDf(rddRow, validatorConfig)
    val column = validatorConfig.getStringList(Constants.Column)
    val expectedNumberOfcolumn = column.size()
    val dfCount = df.count()
    assert(dfCount == recordCount-badRecordCount)
    assert((df.columns(df.columns.size - 1)) == column.get(expectedNumberOfcolumn - 1))
    assert(df.columns.size == expectedNumberOfcolumn)
    val schema = df.schema
    val datatype = validatorConfig.getStringList(Constants.DataType)
    assert(!schema.isEmpty)
    assert(column.size() == schema.length)
    assert(datatype.size() == schema.length)
    val fieldName = schema.fieldNames
    for (i <- 0 until fieldName.length) {
      assert(column.get(i) == fieldName(i))
    }
  }

  test("special character"){
    logger.info("Testing for special character in file")
    val validatorConfig = ValidationPreparer.getValidationConfig()
    val path = "src/test/data/hdfs/speicalCharacter"
    val rddRow = sc.textFile(path).map(x => Row(x))
    val rddWithoutSpecialChar = DataValidator(sc).removeSpecialCharIfAny(rddRow, validatorConfig)
    val specialCharRegex = validatorConfig.getString("specialCharacterReplace")
    val containsSpecialChar = rddWithoutSpecialChar.map(
      x => x.getString(0).contains(specialCharRegex)).collect()
    assert(!containsSpecialChar(0))
    assert(rddWithoutSpecialChar.count == 44)

    logger.info("Testing for special character but not present in file")
    val path1 = "src/test/data/hdfs/source1/"
    val rddRowWOTSC = sc.textFile(path1).map(x => Row(x))
    val rddWithoutSpecialChar1 =
      DataValidator(sc).removeSpecialCharIfAny(rddRowWOTSC, validatorConfig)
    val containsSpecialChar1 = rddWithoutSpecialChar1.map(
      x => x.getString(0).contains(specialCharRegex)).collect()
    assert(!containsSpecialChar1(0))
    assert(rddWithoutSpecialChar1.count == 101)

    logger.info("Testing for special character if regex is not present")
    val path2 = "src/test/data/hdfs/source2/"
    val validatorConfig2 = ValidationPreparer.getValidationConfigSources2
    val rddRowRegexNotPresent = sc.textFile(path2).map(x => Row(x))
    val rddWithoutSpecialChar2 = DataValidator(sc).removeSpecialCharIfAny(
      rddRowRegexNotPresent, validatorConfig2)
    val containsSpecialChar2 = rddWithoutSpecialChar2.map(
      x => x.getString(0).contains(specialCharRegex)).collect()
    assert(!containsSpecialChar2(0))
    assert(rddWithoutSpecialChar2.count == 499)
  }
  test("test error at charAt(0)"){
    val path2 = "src/test/data/hdfs/source5/"
    val rdd = sc.textFile(path2).map(x => Row(x))
    val validatorConfig2 = ValidationPreparer.getValidationConfig(
      "badRecordFilterWorkflow" , "onlyDirTrue")
    val data = DataValidator(sc).validate(rdd, validatorConfig2).collect()
    assert(data(0).toString().contains(
      "BR1.IAD8.ALTER.NET,em0,112,2015-07-07 01:37:08.0,300000.0,719873,299544"))
    assert(data.length == 1 )
  }
}
