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

import javax.servlet.http.HttpServlet

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

/**
 * Created by Pankaj on 5/2/16.
 */
private[framework]
class TestHttpServlet2(sc: SparkContext) extends HttpServlet {

  import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

  import scala.xml.NodeSeq

  val logger = LoggerFactory.getLogger(this.getClass)

  override def service(request: HttpServletRequest, response: HttpServletResponse) {

    logger.info(s"Spark Test: ${sc.sparkUser},${sc.startTime}")
    val parameterNames = request.getParameterNames

    logger.info("**************Parameter********************")
    while(parameterNames.hasMoreElements){
      val nextElement = parameterNames.nextElement()
      logger.info(s"${nextElement}")
      logger.info(s"${request.getParameter(nextElement)}")
    }

    logger.info("**************Header********************")
    val headerNames = request.getHeaderNames
    while(headerNames.hasMoreElements) {
      val nextElement = headerNames.nextElement()
      logger.info(s"${nextElement}")
      logger.info(s"${request.getHeader(nextElement)}")
    }



    response.setContentType("text/html")
    response.setCharacterEncoding("UTF-8")
    val responseBody: NodeSeq =
      <html>
        <head>
          <title>embedded jetty</title>
          <link rel="stylesheet" type="text/css" href="/css/style.css" />
        </head>
        <body>
          <h1>Servlet 2!</h1>
        </body>
      </html>
    response.getWriter.write(responseBody.toString)

  }

}
