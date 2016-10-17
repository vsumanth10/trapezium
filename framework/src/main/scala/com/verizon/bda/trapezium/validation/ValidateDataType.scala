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
import java.util

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

// scalastyle:off
import scala.collection.JavaConversions.asScalaBuffer
// scalastyle:on
import java.lang.{Double, Long}
import java.util.List
import java.sql.{Date, Timestamp}
/**
  * This class validate type of validation mentioned on config files
  *
  */
object ValidateDataType {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * Validate data based on validation mentioned in config.
    * @param arrLine input line
    * @return  Seq[Any] arrLine
    */
  def validate(arrLine: Array[String] , column: List[String] ,
               datatype: List[String] , validation: Config ,
               sformat: SimpleDateFormat , delimiter: Char) :
          Seq[Any] = {
    val outPut = Array.ofDim[Any](column.length)
    val columnItr = column.iterator()
    try {
      while (columnItr.hasNext) {
        val colName = columnItr.next()
        val index = column.indexOf(colName)
         // Moved out of loop. Datatype validation is now by default.
        outPut(index) = checkDatatype(datatype(index).toLowerCase,
          arrLine(index).trim, sformat, colName)
        // Added as part of jira CORE-99
        if (validation.hasPath(colName)) {
        val validationList = validation.getStringList(colName).map(x => x.toLowerCase)
          validationList.map {
            case Constants.CheckSpace =>
              checkSpace(arrLine(index), colName)
            case s if s.contains(Constants.MaxLength) =>
              maxLength(arrLine(index), s.substring(10, s.length() - 1), colName)
            case s if s.contains(Constants.MinLength) =>
              minLength(arrLine(index), s.substring(10, s.length() - 1), colName)
          }
        }
      }
    } catch {
      case ex: Exception =>
        val errLine = arrLine.mkString(delimiter.toString)
        logger.warn(" Row is filtered out because" +
          " : " + ex.getStackTrace.take(500)  +" . Line: " + errLine )
        throw new Exception("Column Data is not valid" + ex)
    }
    outPut.toSeq
  }

  private [validation]def checkDatatype(dataType: String , data: String ,
                                        sformat: SimpleDateFormat, colName : String = ""): Any = {
    try {
      dataType match {
        case Constants.StringType => checkString(data)
        case Constants.IntegerType => checkInt(data)
        case Constants.DateType => checkDate(data, sformat)
        case Constants.TimestampType => checkTimestamp(data, sformat)
        case Constants.LongType => checkLong(data)
        case Constants.DoubleType => checkDouble(data)
        case _ => new Exception ("DataType in config is not correct. " +
          "Please provide is correct datatype in config" +
          ". =>validation.datatypes.")
      }
    } catch {
      case ex: Exception =>
        throw new Exception("data type validation failed for record: " + data +
          " , expected datatype: " + dataType +" : " +"Column name :" +
          colName + "exception" + ex.getStackTrace.take(100) )
      }
  }

  private [validation]def checkString(str: String): String = {
    if (str != null) {
      str.trim
    } else {
      throw new Exception(" checkString failed. Data  " + str + "Column name :" )
    }
  }

  private [validation]def checkSpace(str: String, colName : String = ""): Boolean = {
    if (str != null && str.trim().length() != 0) {
      true
    } else {
      throw new Exception(" checkSpace failed for record : " + str + " colName " + colName )
    }
  }

  private [validation]def maxLength(str: String , max: String, colName : String = ""): Boolean = {
    if (str != null && str.trim().length() != 0 && str.length() <= max.toInt) {
      true
    } else {
      throw new Exception(" validation maxLength failed for record : "
          + str + " , maximum len required :" + max + " colName " + colName )
    }
  }


  private [validation] def minLength(str: String, min: String, colName : String = ""): Boolean = {
    if (str != null && str.trim().length() != 0 && str.length() >= min.toInt) {
      true
    } else {
      throw new Exception("validation minLength failed for record : " + str +
        " , minimum length required :" + min + " colName " + colName )
    }
  }

  private [validation]def checkInt(str: String): Int = {
    if (str != null ) {
      Integer.parseInt(str)
      str.toInt
    } else {
      throw new Exception("checkInt fails for record : " + str)
    }
  }

  private [validation]def checkTimestamp(str: String,
                                         sformat: SimpleDateFormat): Timestamp = {
    try {
      new Timestamp(sformat.parse(str).getTime)
    } catch {
      case ex: Exception => {
        throw new Exception("checkDate failed for record : " + str)
      }
    }
  }

  private [validation]def checkDate(str: String ,
                                    sformat: SimpleDateFormat): Date = {
    try {
      new Date(sformat.parse(str).getTime)
    } catch {
      case ex: Exception => {
        throw new Exception("checkDate failed for record : " + str)
      }
    }
  }

  private [validation]def checkDouble(str: String): Double = {
    if (str != null ) {
      Double.parseDouble(str)
    } else {
      throw new Exception("checkDouble failed for record : " + str)
    }
  }
  private [validation]def checkLong(str: String): Long = {
    if (str != null) {
      Long.parseLong(str)
    } else {
      throw new Exception("checkLong failed for record : " + str)
    }
  }
}
