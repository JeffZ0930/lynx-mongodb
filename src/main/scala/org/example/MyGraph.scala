//Scala-sdk-2.12.17
package org.example

import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.runner.{CypherRunner, GraphModel, NodeFilter, RelationshipFilter, WriteTask}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property._
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPath, LynxPropertyKey, LynxRelationship, LynxRelationshipType, PathTriple}
import org.grapheco.lynx.types.time.LynxDate
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import java.time.LocalDate
import java.util.Date

import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class MyGraph extends GraphModel {

  private val mongoClient = MongoClient("mongodb://root:Hc1478963!@223.193.3.252:27017")

  // private val mongoClient = MongoClient("mongodb://localhost:27017")
  private val database = mongoClient.getDatabase("test")

  private def myNodeAt(id: Int, tableList:Array[String]):Option[MyNode] = {
    println("myNodeAt()")

    for (tableName <- tableList) {
      val collection = database.getCollection(tableName)
      val findObservable = collection.find(equal("_id", id))
      val docList = Await.result(findObservable.toFuture(), 10.seconds)

      if (docList.nonEmpty) {
        val result = rowToNode(docList.head, tableName, nodeSchema(tableName))

        return Some(result)
      }
    }

    None
  }

  private def rowToNode(row: Document, tableName: String, config: Array[(String, String)]): MyNode = {
    val propertyMap = config.indices.map { i =>
      val columnName = config(i)._1
      val columnType = config(i)._2
      val rawValue = row.get(columnName)
      val columnValue =
        try {
          columnType match {
            case "BIGINT" => LynxValue(rawValue.get.asInt64().getValue)
            case "INT" => LynxInteger(rawValue.get.asInt32().getValue)
            case "String" => LynxString(rawValue.get.asString().getValue)
            case _ => LynxString(rawValue.get.asString().getValue)
          }
        } catch {
            case _: NullPointerException => LynxNull
        }
      LynxPropertyKey(columnName) -> columnValue
    }.toMap

    val id = MyId(row.get("_id").get.asInt32().getValue)
    val label = Seq(LynxNodeLabel(tableName))
    MyNode(id, label, propertyMap)
  }

  private def rowToRel(row: Document, tableName: String, config: Array[(String, String)]): MyRelationship = {
    val propertyMap = config.indices.map { i =>
      val columnName = config(i)._1
      val columnType = config(i)._2
      val rawValue = row.get(columnName)
      val columnValue =
        try {
          columnType match {
            case "BIGINT" => LynxValue(rawValue.get.asInt64().getValue)
            case "INT" => LynxInteger(rawValue.get.asInt32().getValue)
            case "String" => LynxString(rawValue.get.asString().getValue)
            case _ => LynxString(rawValue.get.asString().getValue)
          }
        } catch {
          case _: NullPointerException => LynxNull
        }
      LynxPropertyKey(columnName) -> columnValue
    }.toMap

    val id = MyId(row.get("_id").get.asInt32().getValue)
    val startId = MyId(row.get("startId").get.asInt32().getValue)
    val endId = MyId(row.get("endId").get.asInt32().getValue)

    MyRelationship(id, startId, endId, Some(LynxRelationshipType(tableName)), propertyMap)
  }

  private val nodeSchema = Map(
    "Person" -> Array(("_id", "INT"), ("LABEL", "String"), ("creationDate", "Date"), ("firstName", "String"),
      ("lastName", "String"), ("gender", "String"), ("birthday", "Date"), ("locationIP", "String"), ("browserUsed", "String"),
      ("languages", "String"), ("emails", "String")),
    "Place" -> Array(("_id", "INT"), ("LABEL", "String"), ("name", "String"), ("url", "String"), ("type", "String")),
  )

  private val relSchema = Map(
    "knows" -> Array(("_id", "INT"), ("LABEL", "String"), ("creationDate", "Date"), ("startId", "INT"), ("endId", "INT")),
    "isLocatedIn" -> Array(("_id", "INT"), ("LABEL", "String"), ("startId", "INT"), ("endId", "INT"), ("creationDate", "Date")),
  )

  private val relMapping = Map(
    "isLocatedIn" -> (Array("Person", "Comment", "Post", "Organisation"), Array("Place")),
    "replyOf" -> (Array("Comment"), Array("Comment", "Post")), "containerOf" -> (Array("Forum"), Array("Post")),
    "hasCreator" -> (Array("Comment", "Post"), Array("Person")), "hasInterest" -> (Array("Person"), Array("Tag")),
    "workAt" -> (Array("Person"), Array("Organisation")), "hasModerator" -> (Array("Forum"), Array("Person")),
    "hasTag" -> (Array("Comment", "Post", "Forum"), Array("Tag")), "hasType" -> (Array("Tag"), Array("Tagclass")),
    "isSubclassOf" -> (Array("Tagclass"), Array("Tagclass")), "isPartOf" -> (Array("Place"), Array("Place")),
    "likes" -> (Array("Person"), Array("Comment", "Post")), "knows" -> (Array("Person"), Array("Person")),
    "studyAt" -> (Array("Person"), Array("Organisation")), "hasMember" -> (Array("Forum"), Array("Person")),
  )

  override def write: WriteTask = new WriteTask {
    override def createElements[T](nodesInput: Seq[(String, NodeInput)], relationshipsInput: Seq[(String, RelationshipInput)], onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T = ???

    override def deleteRelations(ids: Iterator[LynxId]): Unit = ???

    override def deleteNodes(ids: Seq[LynxId]): Unit = ???

    override def updateNode(lynxId: LynxId, labels: Seq[LynxNodeLabel], props: Map[LynxPropertyKey, LynxValue]): Option[LynxNode] = ???

    override def updateRelationShip(lynxId: LynxId, props: Map[LynxPropertyKey, LynxValue]): Option[LynxRelationship] = ???

    override def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)], cleanExistProperties: Boolean): Iterator[Option[LynxNode]] = ???

    override def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = ???

    override def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(LynxPropertyKey, Any)]): Iterator[Option[LynxRelationship]] = ???

    override def setRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = ???

    override def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxNode]] = ???

    override def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[LynxNodeLabel]): Iterator[Option[LynxNode]] = ???

    override def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[LynxPropertyKey]): Iterator[Option[LynxRelationship]] = ???

    override def removeRelationshipsType(relationshipIds: Iterator[LynxId], typeName: LynxRelationshipType): Iterator[Option[LynxRelationship]] = ???

    override def commit: Boolean = {true}
  }

  override def nodeAt(id: LynxId): Option[MyNode] = ???

  override def nodes(): Iterator[MyNode] = {
    println("nodes()")
    val allNodes = for (tableName <- nodeSchema.keys) yield {
      val collection = database.getCollection(tableName)
      val findObservable = collection.find()
      val documentsList = Await.result(findObservable.toFuture(), 10.seconds)
      documentsList.iterator.flatMap { doc =>
        val node = rowToNode(doc, tableName, nodeSchema(tableName))
        Iterator(node)
      }
    }
    println("nodes() finished")
    allNodes.flatten.iterator
  }

  override def nodes(nodeFilter: NodeFilter): Iterator[MyNode] = {
    println("nodes(NodeFilter)")

    if (nodeFilter.labels.isEmpty && nodeFilter.properties.isEmpty) {
      return nodes()
    }

    val tableName = nodeFilter.labels.head.toString()
    val collection = database.getCollection(tableName)

    val filters = nodeFilter.properties.map { case (field, value) =>
      equal(field.toString(), value.value)
    }.toList

    val findObservable = if (filters.nonEmpty) {
      val finalFilter = and(filters: _*)
      collection.find(finalFilter)
    } else {
      collection.find()
    }

    val documentsList = Await.result(findObservable.toFuture(), 10.seconds)

    val result = documentsList.iterator.flatMap { doc =>
      val node = rowToNode(doc, tableName, nodeSchema(tableName))
      Iterator(node)
    }

    println("nodes(NodeFilter) finished")
    result
  }

  override def relationships(): Iterator[PathTriple] = {
    println("relationships()")

    val allRels = for (tableName <- relSchema.keys) yield {
      val collection = database.getCollection(tableName)
      val findObservable = collection.find()
      val documentsList = Await.result(findObservable.toFuture(), 10.seconds)

      documentsList.iterator.flatMap { doc =>
        val rel = rowToRel(doc, tableName, relSchema(tableName))
        val startNode = myNodeAt(doc.get("startId").get.asInt32().getValue, relMapping(tableName)._1).get
        val endNode = myNodeAt(doc.get("endId").get.asInt32().getValue, relMapping(tableName)._2).get
        Iterator(PathTriple(startNode, rel, endNode))
      }
    }

    println("relationships() finished")
    allRels.flatten.iterator
  }

  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships(RelFilter)")

    val tableName = relationshipFilter.types.head.toString()
    val collection = database.getCollection(tableName)

    val filters = relationshipFilter.properties.map { case (field, value) =>
      equal(field.toString(), value.value)
    }.toList

    val findObservable = if (filters.nonEmpty) {
      val finalFilter = and(filters: _*)
      collection.find(finalFilter)
    } else {
      collection.find()
    }

    val documentsList = Await.result(findObservable.toFuture(), 10.seconds)

    val result = documentsList.iterator.flatMap { doc =>
      val rel = rowToRel(doc, tableName, relSchema(tableName))
      val startNode = myNodeAt(doc.get("startId").get.asInt32().getValue, relMapping(tableName)._1).get
      val endNode = myNodeAt(doc.get("endId").get.asInt32().getValue, relMapping(tableName)._2).get
      Iterator(PathTriple(startNode, rel, endNode))
    }

    println("relationships(RelFilter) finished")
    result
  }

  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    println("expand()")
    if (direction == BOTH) {
      return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING)
    }

    val relType = filter.types.head.toString()
    val startId = id.toLynxInteger.value.toInt
    val startNode = direction match {
      case OUTGOING => myNodeAt(startId, relMapping(relType)._1)
      case INCOMING => myNodeAt(startId, relMapping(relType)._2)
    }
    if (startNode.isEmpty) {
      return Iterator.empty
    }

    val filters = filter.properties.map { case (field, value) =>
      equal(field.toString(), value.value)
    }.toList :+ (direction match {
      case OUTGOING => equal("startId", startId)
      case INCOMING => equal("endId", startId)
    })
    val finalFilter = and(filters: _*)

    val collection = database.getCollection(relType)
    val findObservable = collection.find(finalFilter)
    val documentsList = Await.result(findObservable.toFuture(), 10.seconds)

    val result = documentsList.iterator.flatMap { doc =>
      val endNode = direction match {
        case OUTGOING => myNodeAt(doc.get("endId").get.asInt32().getValue, relMapping(relType)._2).get
        case INCOMING => myNodeAt(doc.get("startId").get.asInt32().getValue, relMapping(relType)._1).get
      }
      Iterator(PathTriple(startNode.get, rowToRel(doc, relType, relSchema(relType)), endNode))
    }

    println("expand() finished")
    result
  }

  private val runner = new CypherRunner(this)

  def run(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = runner.run(query, param)
}


