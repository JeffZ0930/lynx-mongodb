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

  private val mongoClient = MongoClient("mongodb://admin:admin123@10.0.82.144:27017")
  private val database = mongoClient.getDatabase("db1")

  def myNodeAt(id: String, tableList:Array[String]):Option[MyNode] = {
    println("myNodeAt()")
    val startTime1 = System.currentTimeMillis()

    for (tableName <- tableList) {
      val collection = database.getCollection(tableName)
      val findObservable = collection.find(equal("id:ID", id))
      val docList = Await.result(findObservable.toFuture(), 300.seconds)

      if (docList.nonEmpty) {
        val result = rowToNode(docList.head, tableName, nodeSchema(tableName))

        println("myNodeAt finished in " + (System.currentTimeMillis() - startTime1) + " ms")
        return Some(result)
      }
    }

    None
  }

  private def rowToNode(row: Document, tableName: String, config: Array[(String, String)]): MyNode = {
    val propertyMap = config.indices.map { i =>
      val columnName = config(i)._1
      val columnType = config(i)._2
      val rawValue = row.get(columnName).get
      val columnValue = if (rawValue.isNull) {
        LynxNull
      } else {
          columnType match {
            // case "INT" => LynxInteger(rawValue.get.asInt32().getValue)
            case "String" => LynxString(rawValue.asString().getValue)
            case _ => LynxString(rawValue.asString().getValue)
          }
      }

      LynxPropertyKey(columnName) -> columnValue
    }.toMap

    val id = MyId(row.get("id:ID").get.asString().getValue.toLong)
    val label = Seq(LynxNodeLabel(tableName))
    MyNode(id, label, propertyMap)
  }

  private def rowToRel(row: Document, tableName: String, config: Array[(String, String)]): MyRelationship = {
    val propertyMap = config.indices.map { i =>
      val columnName = config(i)._1
      val columnType = config(i)._2
      val rawValue = row.get(columnName).get
      val columnValue = if (rawValue.isNull) {
        LynxNull
      } else {
        columnType match {
          // case "INT" => LynxInteger(rawValue.get.asInt32().getValue)
          case "String" => LynxString(rawValue.asString().getValue)
          case _ => LynxString(rawValue.asString().getValue)
        }
      }
      LynxPropertyKey(columnName) -> columnValue
    }.toMap

    val id = MyId(row.get("REL_ID").get.asString().getValue.toLong)
    val startId = MyId(row.get(":START_ID").get.asString().getValue.toLong)
    val endId = MyId(row.get(":END_ID").get.asString().getValue.toLong)

    MyRelationship(id, startId, endId, Some(LynxRelationshipType(tableName)), propertyMap)
  }

  private val nodeSchema = Map(
    "Person" -> Array(("id:ID", "String"), (":LABEL", "String"), ("creationDate", "Date"), ("firstName", "String"),
      ("lastName", "String"), ("gender", "String"), ("birthday", "Date"), ("locationIP", "String"), ("browserUsed", "String"),
      ("languages", "String"), ("emails", "String")),
    "Place" -> Array(("id:ID", "String"), (":LABEL", "String"), ("name", "String"), ("url", "String"), ("type", "String")),
    "Organisation" -> Array(("id:ID", "String"), (":LABEL", "String"), ("type", "String"), ("name", "String"), ("url", "String")),
    "Comment" -> Array(("id:ID", "String"), (":LABEL", "String"), ("creationDate", "Date"), ("locationIP", "String"),
      ("browserUsed", "String"), ("content", "String"), ("length", "INT")),
    "Post" -> Array(("id:ID", "String"), ("creationDate", "Date"), (":LABEL", "String"), ("imageFile", "String"),
      ("locationIP", "String"), ("browserUsed", "String"), ("language", "String"), ("content", "String"), ("length", "INT")),
    "Forum" -> Array(("id:ID", "String"), ("creationDate", "Date"), (":LABEL", "String"), ("title", "String")),
    "Tag" -> Array(("id:ID", "String"), (":LABEL", "String"), ("name", "String"), ("url", "String")),
    "Tagclass" -> Array(("id:ID", "String"), (":LABEL", "String"), ("name", "String"), ("url", "String"))
  )

  private val relSchema = Map(
    "knows" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isLocatedIn" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("creationDate", "Date")),
    "containerOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasCreator" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasInterest" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasMember" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasModerator" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasTag" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "hasType" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isPartOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "isSubclassOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "likes" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "replyOf" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT")),
    "studyAt" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("classYear", "INT")),
    "workAt" -> Array(("REL_ID", "BIGINT"), (":TYPE", "String"), ("creationDate", "Date"), (":START_ID", "BIGINT"), (":END_ID", "BIGINT"), ("workFrom", "INT"))
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
  override def nodeAt(id: LynxId): Option[LynxNode] = ???

  override def nodes(): Iterator[MyNode] = {
    println("nodes()")
    val allNodes = for (tableName <- nodeSchema.keys) yield {
      val collection = database.getCollection(tableName)
      val findObservable = collection.find()
      val documentsList = Await.result(findObservable.toFuture(), 300.seconds)
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
    val startTime1 = System.currentTimeMillis()
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
    println("checkpoint1: " + (System.currentTimeMillis() - startTime1) + " ms")
    val documentsList = Await.result(findObservable.toFuture(), 300.seconds)
    println("checkpoint2: " + (System.currentTimeMillis() - startTime1) + " ms")
    val result = documentsList.iterator.flatMap { doc =>
      val node = rowToNode(doc, tableName, nodeSchema(tableName))
      Iterator(node)
    }

    println("nodes(nodeFilter) finished in " + (System.currentTimeMillis() - startTime1) + " ms")
    result
  }

  override def relationships(): Iterator[PathTriple] = {
    println("relationships()")

    val allRels = for (tableName <- relSchema.keys) yield {
      println(s"Getting from $tableName")
      val collection = database.getCollection(tableName)
      val findObservable = collection.find()
      val documentsList = Await.result(findObservable.toFuture(), 300.seconds)

      documentsList.iterator.flatMap { doc =>
        val rel = rowToRel(doc, tableName, relSchema(tableName))
        val startNode = myNodeAt(doc.get(":START_ID").get.asString().getValue, relMapping(tableName)._1).get
        val endNode = myNodeAt(doc.get(":END_ID").get.asString().getValue, relMapping(tableName)._2).get
        Iterator(PathTriple(startNode, rel, endNode))
      }
    }

    println("relationships() finished")
    allRels.flatten.iterator
  }

  override def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = {
    println("relationships(RelFilter)")
    val startTime1 = System.currentTimeMillis()

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

    val documentsList = Await.result(findObservable.toFuture(), 300.seconds)

    val result = documentsList.iterator.flatMap { doc =>
      val rel = rowToRel(doc, tableName, relSchema(tableName))
      val startNode = myNodeAt(doc.get(":START_ID").get.asString().getValue, relMapping(tableName)._1).get
      val endNode = myNodeAt(doc.get(":END_ID").get.asString().getValue, relMapping(tableName)._2).get
      Iterator(PathTriple(startNode, rel, endNode))
    }

    println("relationships(RelFilter) finished in " + (System.currentTimeMillis() - startTime1) + " ms")
    result
  }

  override def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] = {
    println("expand()")
    val startTime1 = System.currentTimeMillis()

    if (direction == BOTH) {
      return expand(id, filter, OUTGOING) ++ expand(id, filter, INCOMING)
    }

    val relType = filter.types.head.toString()
    val startId = id.toString()
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
      case OUTGOING => equal(":START_ID", startId)
      case INCOMING => equal(":END_ID", startId)
    })
    val finalFilter = and(filters: _*)

    val collection = database.getCollection(relType)
    val findObservable = collection.find(finalFilter)
    val documentsList = Await.result(findObservable.toFuture(), 300.seconds)

    val result = documentsList.iterator.flatMap { doc =>
      val endNode = direction match {
        case OUTGOING => myNodeAt(doc.get(":END_ID").get.asString().getValue, relMapping(relType)._2).get
        case INCOMING => myNodeAt(doc.get(":START_ID").get.asString().getValue, relMapping(relType)._1).get
      }
      Iterator(PathTriple(startNode.get, rowToRel(doc, relType, relSchema(relType)), endNode))
    }

    println("expand() finished in " + (System.currentTimeMillis() - startTime1) + " ms")
    result
  }

  private val runner = new CypherRunner(this)

  def run(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult = runner.run(query, param)
}

