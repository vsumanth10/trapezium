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
package com.verizon.bda.trapezium.framework.server

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.verizon.bda.trapezium.framework.utils.ReflectionSupport
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

/**
  * Created by Jegan on 5/24/16.
  */
class AkkaRouteHandler(sc: SparkContext, as: ActorSystem) extends ReflectionSupport {

  val logger = LoggerFactory.getLogger(this.getClass)
  val exceptionHandler = AkkaHttpExceptionHandler.handler

  def defineRoute(path: String, endPointClassName: String): Route = {
    require(path != null && !path.isEmpty, "Path cannot be null or empty")
    require(endPointClassName != null && !endPointClassName.isEmpty,
      "End-point class name cannot be null or empty")

    val endPoint = loadEndPoint(endPointClassName)
    pathPrefix(path) {
      handleExceptions(exceptionHandler) {
        endPoint.route
      }
    }
  }

  private def loadEndPoint(endPointClassName: String): ServiceEndPoint = {
    val clazz = loadClass(endPointClassName)

    /*
    * Create instance of endpoint using the constructor either the one with having
    * SparkContext param or the one with ActorSystem
    */
    val instance = instanceOf[SparkContext](clazz, classOf[SparkContext], sc) orElse
      instanceOf[ActorSystem](clazz, classOf[ActorSystem], as)

    instance.getOrElse {
      logger.error(
        "EndPoint should have constructors with params of either SparakContext or ActorSystem",
        "Unable to create endpoint instances")
      // Can't do much. Exit with error.
      throw new RuntimeException("Unable to create endpoint instances")
    }
  }

  private def instanceOf[T](clazz: Class[_], paramType: Class[T], param: AnyRef):
  Option[ServiceEndPoint] = {
    val cons = getConstructorOfType(clazz, paramType)
    cons.map(c => c.newInstance(param).asInstanceOf[ServiceEndPoint])
  }
}
