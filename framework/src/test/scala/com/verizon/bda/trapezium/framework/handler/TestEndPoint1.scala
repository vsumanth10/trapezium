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

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import com.verizon.bda.trapezium.framework.server.{ActorServiceEndPoint, SparkServiceEndPoint}
import org.apache.spark.SparkContext

/**
  * Created by Jegan on 5/19/16.
  */
class TestEndPoint1(sc: SparkContext) extends SparkServiceEndPoint(sc) {
  override def route: server.Route = {
    path("test") {
      get {
        complete {
          "Hello from EndPoint 1"
        }
      }
    }
  }
}

class TestEndPoint2(sc: SparkContext) extends SparkServiceEndPoint(sc) {
  override def route: server.Route = {
    path("test2") {
      get {
        complete {
          "Hello from EndPoint 2"
        }
      }
    }
  }
}

class TestEndPoint3(sc: SparkContext) extends SparkServiceEndPoint(sc) {
  override def route: server.Route = {
    path("test3") {
      get {
        throw new Exception("Test Error")
      }
    }
  }
}

class TestEndPoint4(as: ActorSystem) extends ActorServiceEndPoint(as) {
  override def route: server.Route = {
    path("actortest") {
      get {
        val actor = as.actorOf(Props[TestActor], "test-actor")
        actor ! "Hi"
        complete("Hello from Actor based EndPoint 4")
      }
    }
  }
}

class TestActor extends Actor {
  override def receive: Receive = {
    case _ => {
      sender ! "Hello!!!"
    }
  }
}
