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

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.verizon.bda.trapezium.framework.manager.WorkflowConfig
import org.apache.spark.util.TestUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import spray.json.DefaultJsonProtocol

import scala.collection.mutable.ListBuffer

/**
  * Created by Jegan on 5/20/16.
  */
class AkkaHttpServerTest extends FlatSpec with Matchers with ScalatestRouteTest
  with BeforeAndAfterAll with DefaultJsonProtocol with SprayJsonSupport {

  var sc: SparkContext = _

  var server: AkkaHttpServer = _

  override def beforeAll: Unit = {
    val conf = TestUtils.getSparkConf()
    sc = new SparkContext(conf)

    server = new AkkaHttpServer(sc)

    server.init(config)
    server.start(config)
  }

  override def afterAll: Unit = {
    sc.stop()
    server.stop()
  }

  val config = new WorkflowConfig("akkaHttpServerWorkFlow").httpServer

  "init" should "construct the base routes" in {
    val server = new AkkaHttpServer(sc)
    server.init(config)

    server.routes.size should be (4)
  }

  "start" should "compose the routes correctly" in {
    val route = server.compose(RouteFixture.routes)

    Get("/newtest") ~> route ~> check {
      responseAs[String] shouldEqual "route test completed"
    }

    Get("/newtest2") ~> route ~> check {
      responseAs[String] shouldEqual "route test2 completed"
    }
  }

  "start" should "compose json route correctly" in {
    val route = server.compose(RouteFixture.routes)

    implicit val jsonTestFormat = jsonFormat2(JsonTest)

    Get("/jsontest") ~> route ~> check {
      responseAs[JsonTest] shouldEqual JsonTest("test", 100)
    }
  }

  "server" should "handle exceptions gracefully" in {
    val route = server.compose(RouteFixture.routes)

    implicit val jsonTestFormat = jsonFormat2(JsonTest)
    Get("/exceptiontest") ~> route ~> check {
      response.status.intValue shouldEqual StatusCodes.InternalServerError.intValue
    }
  }
}

object RouteFixture extends SprayJsonSupport with DefaultJsonProtocol {

  val route1 = {
    path("newtest") {
      get {
        complete("route test completed")
      }
    }
  }
  val route2 = {
    path("newtest2") {
      get {
        complete("route test2 completed")
      }
    }
  }

  implicit val format = jsonFormat2(JsonTest)
  val jsonRoute = {
    path("jsontest") {
      get {
        complete(JsonTest("test", 100))
      }
    }
  }

  val exceptionRoute = {
    path("exceptiontest") {
      get {
        throw new Exception("Test Error")
      }
    }
  }

  val routes = ListBuffer(route1, route2, jsonRoute, exceptionRoute).toList

}

case class JsonTest(key: String, value: Int)
