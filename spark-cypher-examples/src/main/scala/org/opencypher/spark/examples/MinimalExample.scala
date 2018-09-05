package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.neo4j.io.testing.Neo4jHarnessUtils._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.testing.support.creation.CAPSNeo4jHarnessUtils._

object MinimalExample extends App {

  implicit val caps = CAPSSession.local()
  val neo4j = startNeo4j("CREATE (:A)").withSchemaProcedure

  println("Here")

  // Exits
//  println(neo4j.dataSourceConfig.cypher("RETURN 1 as foo"))

  // does not exit
//  val ds = Neo4jPropertyGraphDataSource(neo4j.dataSourceConfig)
//  val graph = ds.graph(ds.entireGraphName)
//  graph.nodes("foo").show

  // does not exit
  val nodesDF = SocialNetworkDataFrames.nodes(caps.sparkSession)
  val relsDF = SocialNetworkDataFrames.rels(caps.sparkSession)
  val personTable = CAPSNodeTable(Set("Person"), nodesDF)
  val friendsTable = CAPSRelationshipTable("KNOWS", relsDF)
  val graph = caps.readFrom(personTable, friendsTable)

  val ds = Neo4jPropertyGraphDataSource(neo4j.dataSourceConfig)
  ds.store(GraphName("foo"), graph)

//  neo4j.dataSourceConfig.withSession { session =>
//    session.run("RETURN 1 as foo").consume()
//  }


  neo4j.close()

  println("there")
}
