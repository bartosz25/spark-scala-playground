package com.waitingforcode.sql

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog.{CreateTableEvent, CreateTablePreEvent, ExternalCatalogEvent}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class ExternalCatalogWithListenerTest extends FlatSpec with Matchers {

  private val TestedSparkSession: SparkSession = SparkSession.builder()
    .appName("ExternalCatalogWithListener test").master("local[*]").getOrCreate()
  import TestedSparkSession.implicits._

  "create table events" should "be caught by the listener" in {
    val catalogEvents = new scala.collection.mutable.ListBuffer[ExternalCatalogEvent]()
    TestedSparkSession.sparkContext.addSparkListener(new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case externalCatalogEvent: ExternalCatalogEvent => catalogEvents.append(externalCatalogEvent)
          case _ => {}
        }
      }
    })
    val tableName = s"orders${System.currentTimeMillis()}"
    val orders = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user1")).toDF("order_id", "user_id")

    orders.write.mode(SaveMode.Overwrite).bucketBy(2, "user_id").saveAsTable(tableName)


    val createTablePreEvent = catalogEvents.collectFirst{
      case event if event.isInstanceOf[CreateTablePreEvent] => event.asInstanceOf[CreateTablePreEvent]
    }
    createTablePreEvent shouldBe defined
    createTablePreEvent.get shouldEqual CreateTablePreEvent("default", tableName)
    val createTableEvent = catalogEvents.collectFirst{
      case event if event.isInstanceOf[CreateTableEvent] => event.asInstanceOf[CreateTableEvent]
    }
    createTableEvent shouldBe defined
    createTableEvent.get shouldEqual CreateTableEvent("default", tableName)
  }

}
