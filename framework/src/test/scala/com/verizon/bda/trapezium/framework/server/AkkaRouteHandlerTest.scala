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
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.spark.util.TestUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Jegan on 5/24/16.
  */
class AkkaRouteHandlerTest extends FlatSpec with Matchers with ScalatestRouteTest
  with BeforeAndAfterAll {

  var sc: SparkContext = _

  var as: ActorSystem = _

  override def beforeAll: Unit = {
    val conf = TestUtils.getSparkConf()
    sc = new SparkContext(conf)

    as = ActorSystem("test")
  }

  override def afterAll: Unit = {
    sc.stop()
    as.shutdown()
  }

  val routeHandler = new AkkaRouteHandler(sc, as)

  "defineRoute" should "throw exception on invalid endpoint implementation" in {
    intercept[RuntimeException] {
      val route = routeHandler.defineRoute("test",
        "com.verizon.bda.trapezium.framework.server.InvalidEndPoint")
    }
  }

  "defineRoute" should "throw exception on invalid endpoint configuration" in {
    val config = Table (
      ("path", "className"),
      (null, "com.verizon.bda.trapezium.framework.handler.TestEndPoint1"),
      ("", "com.verizon.bda.trapezium.framework.handler.TestEndPoint1"),
      ("test-path", null),
      ("test-path", "")
    )
    forAll (config) { (path: String, className: String) =>
      intercept[IllegalArgumentException] {
        val route = routeHandler.defineRoute(path, className)
      }
    }
  }
}

// Endpoint without expected constructor
class InvalidEndPoint extends ServiceEndPoint {
  override def route: server.Route = {
    path("invalid") {
      get {
        complete("Hello from Invalid EndPoint")
      }
    }
  }
}
