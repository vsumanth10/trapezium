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

import java.text.SimpleDateFormat
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.streaming.TestSuiteBase
import java.sql.Timestamp

import org.slf4j.LoggerFactory


class ValidateDataTypeSuite extends TestSuiteBase  with Serializable  {

  val logger = LoggerFactory.getLogger(this.getClass)
  val parser = new CSVParser(',')

  val validatorConfig = ValidationPreparer.getValidationConfig()

  val goodRow = "1445263152,61,f1565533d2864e08,\"209.46.48.146\"," +
    "1,\"dt.scanscout.com\",\"/ssframework/cookieSync.htm?redirected" +
    "=1&url=http%3A%2F%2Fsync.adap.tv%2Fsync%3Ftype%3DTYPE%26key%3Dt" +
    "remormedia%26uid%3D%5BUSER_ID%5D\",\"Mozilla/5.0 (iPhone; CPU " +
    "iPhone OS 8_4 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) " +
    "Mobile/12H143 [FBAN/FBIOS;FBAV/41.0.0.22.268;FBBV/15798540;FBDV/iPhone7,2;" +
    "FBMD/iPhone;FBSN/\",\"text/html\",302,0,535,556,\"\",4,\"\",\"TN0\",\"\",0,0"

  val badRowMinsize = "badrecords3e1226 0269c420b7,\"173.192.82.194\",1," +
    "\"realtime.services.disqus.com\",\"/ws/2/thread/4243265377\",\"Mozilla/5.0 " +
    "(iPhone; CPU iPhone OS 9_0_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, " +
    "like Gecko) Mobile/13A452 [FBAN/FBIOS;FBAV/41.0.0.22.268;FBBV/15798540;FBDV/" +
    "iPhone5,2;FBMD/iPhone;FB\",\"text/html\",0,0,583,3500,\"\",4,\"\",\"HI0\",\"\",0,1"

  val badRowDatatype = "1445263152,nonint,1232223333,\"173.192.82.194\",1," +
    "\"realtime.services.disqus.com\",\"/ws/2/thread/4243265377\",\"Mozilla/5.0 " +
    "(iPhone; CPU iPhone OS 9_0_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, " +
    "like Gecko) Mobile/13A452 [FBAN/FBIOS;FBAV/41.0.0.22.268;FBBV/15798540;FBDV/" +
    "iPhone5,2;FBMD/iPhone;FB\",\"text/html\",0,0,583,3500,\"\",4,\"\",\"HI0\",\"\",0,1"
  val arryGoodRec = parser.parseLine(goodRow)
  val arryBadMinsize = parser.parseLine(badRowMinsize)
  val arrybadRowDatatype = parser.parseLine(badRowDatatype)
  val numberOfCol = validatorConfig.getList(Constants.Column).size
  val column = validatorConfig.getStringList(Constants.Column)
  val datatype = validatorConfig.getStringList(Constants.DataType)
  val validation = validatorConfig.getConfig(Constants.Validation)
  val sformat: SimpleDateFormat = new SimpleDateFormat(
    validatorConfig.getString(Constants.DateFormat))
  val delimiter = validatorConfig.getString(Constants.Delimiter).toCharArray
  test("ValidateDataType : Test validate: Validate line as per config ") {
    val seqPostive = ValidateDataType.validate(arryGoodRec, column,
      datatype, validation, sformat, delimiter(0))
    val expectedNumberOfcolumn = column.size()
    assert(expectedNumberOfcolumn == seqPostive.length, "Fail")
    intercept[Exception] {
      logger.info("negative testing for invalid data type")
      ValidateDataType.validate(arrybadRowDatatype, column, datatype, validation,
        sformat, delimiter(0))
    }
    intercept[Exception] {
      logger.info("negative testing for invalid data")
      ValidateDataType.validate(arryBadMinsize, column, datatype, validation,
      sformat, delimiter(0))
    }
    intercept[Exception] {
      logger.info("negative testing for invalid data type")
      ValidateDataType.validate(arrybadRowDatatype, column, datatype, validation,
      sformat, delimiter(0))
    }
  }

  test("ValidateDataType:checkDatatype: Testing Datatype as per config") {

    val st = ValidateDataType.checkDatatype("string", "testString", sformat)
    assert(st.equals("testString"))

    intercept[Exception] {
      val NullString: String = null
      logger.info("negative testing checkDatatype")
      ValidateDataType.checkDatatype("string", NullString, sformat)
    }

    val intval = ValidateDataType.checkDatatype("integer", "1", sformat)
    assert(intval == 1)

    intercept[Exception] {
      logger.info("negative testing checkDatatype")
      ValidateDataType.checkDatatype("integer", "nonInt", sformat)
    }
    val Longval = ValidateDataType.checkDatatype("long", "1000", sformat)

    assert(Longval == 1000L)

    intercept[Exception] {
      logger.info("negative testing checkDatatype")
      ValidateDataType.checkDatatype("long", "nonLong", sformat)
    }
  }

  test("ValidateDataType:checkString: Testing String is not null") {

    assert(ValidateDataType.checkString("StringTest").equals("StringTest"))

    intercept[Exception] {
      val NullString: String = null
      logger.info("negative testing checkString")
      ValidateDataType.checkString(NullString)
    }
  }

  test("ValidateDataType:test checkSpace"){

    assert(ValidateDataType.checkSpace("StringTest"))

    intercept[Exception] {
      logger.info("negative testing checkSpace")
      ValidateDataType.checkSpace("")
    }
  }

  test("ValidateDataType:test maxLength") {

    assert(ValidateDataType.maxLength("StrTest", "10"))

    intercept[Exception] {
      logger.info("negative testing maxLength")
      ValidateDataType.maxLength("StrTest", "1")
    }
  }
  test("ValidateDataType:test minLength") {
    assert(ValidateDataType.minLength("StrTest", "1"))
    intercept[Exception] {
      logger.info("negative testing minLength")
      ValidateDataType.minLength("StrTest", "10")
    }
  }

  test("ValidateDataType:test checkInt"){

    assert(ValidateDataType.checkInt("100") == 100)

    intercept[Exception] {
      logger.info("negative testing checkInt")
      ValidateDataType.checkInt("non int")
    }
  }

  test("ValidateDataType:test timestamp") {

    val dt = ValidateDataType.checkTimestamp("2015-07-07 01:37:08", sformat)
    assert(dt.equals(new Timestamp(sformat.parse("2015-07-07 01:37:08").getTime)))

    intercept[Exception] {
      logger.info("negative testing timestamp")
      ValidateDataType.checkTimestamp("non Date", sformat)
    }
  }



  test("ValidateDataType:test checkDate") {

    val dt = ValidateDataType.checkDate("2015-07-07 01:37:08", sformat)

    assert(dt.toString.equals("2015-07-07"))

    intercept[Exception] {
      logger.info("negative testing checkDate")
      ValidateDataType.checkDate("non Date", sformat)
    }
  }

  test("ValidateDataType:test checkDouble") {

    assert(ValidateDataType.checkDouble("100") == 100)

    intercept[Exception] {
      logger.info("negative testing checkDouble")
      ValidateDataType.checkDouble("non Long")
    }
  }

  test("ValidateDataType:test checkLong") {

    assert(ValidateDataType.checkLong("100") == 100L)

    intercept[Exception] {
      logger.info("negative testing checkLong")
      ValidateDataType.checkLong("non Long")
    }
  }
}
