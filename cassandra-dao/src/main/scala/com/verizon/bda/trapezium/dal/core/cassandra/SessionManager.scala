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
package com.verizon.bda.trapezium.dal.core.cassandra

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.policies.{RoundRobinPolicy, TokenAwarePolicy, DefaultRetryPolicy}
import com.datastax.driver.core._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by v468328 on 3/18/16.
  * It mantains session per combination of cluster and keyspace.
  * Intent here is not to create multiple clusters/sessions per application
  * unless needed for performence.
  */
object SessionManager {
  val sessions = new ListBuffer[Session]()

  def getSession(hosts: mutable.Buffer[String], keyspace: String,
                 cassandraConfig: CassandraConfig =
                 new CassandraConfig(new java.util.HashMap()) ): Session = {


    var retVal: Session = null
    var cluster: Cluster = null;

    for (session <- sessions) {

      if (matchHostWithClusterNames(session.getCluster.getMetadata().getAllHosts(), hosts)) {
        // also check if session is bind to keyspace
        cluster = session.getCluster();
        if (session.getLoggedKeyspace().equals(keyspace)) {
          retVal = session;
        }
      }
    }

    // if we are here it means that either
    if (cluster == null) {
      val builder: Builder = Cluster.builder()
        .addContactPoints(hosts: _*)
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withLoadBalancingPolicy(
          new TokenAwarePolicy(new RoundRobinPolicy()))

      if (null != cassandraConfig && cassandraConfig.configOptions.get("port") != null) {
        builder.withPort(cassandraConfig.configOptions.get("port").asInstanceOf[Int])
      }

      if (null != cassandraConfig &&
        cassandraConfig.configOptions.get("consistencyLevel") != null) {
        val qOptions: QueryOptions = new QueryOptions()
        qOptions.setConsistencyLevel(cassandraConfig.configOptions.get("consistencyLevel").
          asInstanceOf[ConsistencyLevel])
        builder.withQueryOptions(qOptions)
      }
      cluster = builder.build()

      // set kespace and return session
      retVal = cluster.connect(keyspace)
      sessions.append(retVal)
    }
    else if (retVal == null) {
      // it means we have cluster but no session
      retVal = cluster.connect(keyspace)
      sessions.append(retVal)
    }
    retVal
  }

  /**
    * It should be called once per application because it will shutdown all sessions and clusters.
    */
  def shutdown(): Unit = {

    for (session <- sessions) {
      val cluster: Cluster = session.getCluster;

      if (!session.isClosed()) {
        session.close();
      }
      if (!cluster.isClosed()) {
        cluster.close();
      }


    }
    // in end clear out all sessions
    sessions.clear();
  }

  def matchHostWithClusterNames(hostListFromSession: java.util.Set[Host],
                                hostListFromCaller: mutable.Buffer[String]): Boolean = {


    val walker = hostListFromSession.iterator
    while (walker.hasNext) {
      val host = walker.next()
      if (hostListFromCaller.contains(host.getAddress().getHostName())) {
        return true;
      }

    }
    return false;

  }


}

/**
  * It is helper method. The reason it is int its own class is it is passed to worker
  * node and we objects are serializable
  */
object DAOHelper {

  def convert(list: java.util.List[String]): ListBuffer[String] = {

    val buffer: ListBuffer[String] = new ListBuffer();
    val walker: java.util.Iterator[String] = list.iterator();
    while (walker.hasNext) {
      buffer.append(walker.next());
    }
    buffer;

  }

}

/**
  * Config parameters for Cassandra.
  */
case class CassandraConfig( configOptions: java.util.HashMap[String, Object])




