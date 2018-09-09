package com.waitingforcode.structuredstreaming.join

/**
  * Created by bartosz on 28/07/18.
  */
class StateManagementInnerJoinTest {

}

/**

// TODO : in the post show each test separetly and before showing it, put an image to really show what it illustrates
  // TODO : logically it should return fewer rows but it's not the case...
  // TODO : maybe the reason is that main key's watermark is updated every time when a joined key arrives ?
  it should "get fewer rows for key1 event with watermark on both sides" in {
    sparkSession.stop()
    val runCounters = new mutable.ListBuffer[Int]()
    for (runId <- 0 to 1) {
      val newSparkSession: SparkSession = SparkSession.builder()
        .appName("Spark Structured Streaming inner join test")
        .master("local[*]").getOrCreate()
      import newSparkSession.implicits._
      val mainEventsStream = new MemoryStream[MainEvent](1+runId*2, newSparkSession.sqlContext)
      val joinedEventsStream = new MemoryStream[JoinedEvent](2+runId*2, newSparkSession.sqlContext)
      val (mainEventsDataset, joinedEventsDataset, withWatermarkFlag) = if (runId == 0) {
        println("WATERMARK 2 seconds")
        (mainEventsStream.toDS().withWatermark("mainEventTimeWatermark", "2 seconds").repartition(1),
          joinedEventsStream.toDS().withWatermark("joinedEventTimeWatermark", "2 seconds").repartition(1), true)
      } else {
        println("NO WATERMARK")
        (mainEventsStream.toDS().repartition(1), joinedEventsStream.toDS().repartition(1), false)
      }
      val stream = mainEventsDataset.join(joinedEventsDataset, mainEventsDataset("mainKey") === joinedEventsDataset("joinedKey"))
      // TODO : it seems that it computes watermark for the main side here ==> if I don't send any new events on it,
      //        the watermark value doesn't change - despite the fact that joined side is still receiving new data

      val query = stream.writeStream.trigger(Trigger.ProcessingTime(2000L)).foreach(RowProcessor).start()

      new Thread(new Runnable() {
        override def run(): Unit = {
          while (!query.isActive) {}
          var wasSent = false
          var processingTimeFrom1970 = 10000L
          mainEventsStream.addData(MainEvent(s"key1", processingTimeFrom1970, new Timestamp(processingTimeFrom1970)))
          joinedEventsStream.addData(Events.joined(s"key1", eventTime = processingTimeFrom1970))
          TestedValuesContainer.key1Counter.incrementAndGet()

          if (withWatermarkFlag) {
            println(s"last query eventtime=${query.lastProgress.eventTime}")
            while (query.lastProgress.eventTime.toString == "{watermark=1970-01-01T00:00:00.000Z}") {
              println(s"wait for progress ${query.lastProgress.eventTime}")
              Thread.sleep(2000L)
              if (!wasSent) {
                wasSent = true
                mainEventsStream.addData(MainEvent(s"key1", processingTimeFrom1970, new Timestamp(processingTimeFrom1970)))
                joinedEventsStream.addData(Events.joined(s"key1", eventTime = processingTimeFrom1970))
                TestedValuesContainer.key1Counter.incrementAndGet()
              }
            }
            println(s"progress changed ! ${query.lastProgress.eventTime}")
            val stateOps = query.lastProgress.stateOperators.mkString(", ")
            println(s"statops=${stateOps}")
          }
          while (query.isActive) {
            //Thread.sleep(1000L)
            // Logically key1 should expire after 10 seconds so joined key shouldn't be really joined because of the watermark
            // on the main event
            // We should then receive only 10 rows for "with watermark" and more than that for "without watermark"
            //if (processingTimeFrom1970 > 10000) {
            joinedEventsStream.addData(Events.joined(s"key1", eventTime = processingTimeFrom1970 - 50000L))
            TestedValuesContainer.key1Counter.incrementAndGet()
            //}
            processingTimeFrom1970 += 1000L
            if (processingTimeFrom1970 % 10000 == 0) {
              mainEventsStream.addData(MainEvent(s"key${System.currentTimeMillis()}", processingTimeFrom1970, new Timestamp(processingTimeFrom1970)))
              Thread.sleep(2000L)
              //processingTimeFrom1970 += 5000L
            }
          }
        }
      }).start()
      //val sender = new OneKeyDataSender(query, mainEventsStream, joinedEventsStream)
      // sender.thread.start()
      query.awaitTermination(120000)
      mainEventsStream.stop()
      joinedEventsStream.stop()
      query.stop()

      //sender.thread.
      val key1Count = TestedValuesContainer.key1Counter.get()
      runCounters.append(key1Count)

      println(s"key1Count=${key1Count}")
      val groupedByKeys = TestedValuesContainer.values.groupBy(testedValues => testedValues.key)
      println(s"key1 size=${groupedByKeys("key1").size} vs ${key1Count}")
      runCounters.append(groupedByKeys("key1").size)
      TestedValuesContainer.values.clear()
      TestedValuesContainer.key1Counter.set(0)
      println(s"reset counter=${TestedValuesContainer.key1Counter.get()}")
      println(s"reset counter=${TestedValuesContainer.values}")
      newSparkSession.close()
      Thread.sleep(10000L)
    }
    println(s"runCounter=${runCounters}")
    /*
    // no watermark run: key1Count=117
    //key1 size=96 vs 117
    // watermark run:   key1Count=116  key1 size=59 vs 116
    // During my tests the number of keys sent by the test without watermark was 117 and for the test
    // with watermark 116 = for flexibility, I allow here a difference o 10 events
    val sentKeysDifference = (runCounters(0) - runCounters(2)).abs
    (sentKeysDifference >= 0 && sentKeysDifference <= 10) shouldBe true
    // In the tests I got: 96 processed messages for "without watermark" scenario and
    // 59 for "with watermark" scenario = for flexibility, I allow the difference of at least 30
    println(s"Got diff = ${runCounters}")
    val processedKeysDifference = runCounters(1) - runCounters(3)
    processedKeysDifference should be >= 30*/

    // Measure with watermark (test executed separetly): 233 vs 188 processed
    // Measure without watermark (test executed separetly): 233 vs 222 processed
    // 2ND test
    // No watermark: runCounter=ListBuffer(353, 324)
    // with watermark: runCounter=ListBuffer(353, 322)
  }

  // TODO : add test for watermark

  it should "ignore rows behind range condition query" in {
    val mainEventsStream = new MemoryStream[MainEvent](1, sparkSession.sqlContext)
    val joinedEventsStream = new MemoryStream[JoinedEvent](2, sparkSession.sqlContext)

    val mainEventsDataset = mainEventsStream.toDS()
    val joinedEventsDataset = joinedEventsStream.toDS()
    val stream = mainEventsDataset.join(joinedEventsDataset, expr(
      """
        | mainKey = joinedKey AND
        | joinedEventTimeWatermark > mainEventTimeWatermark AND joinedEventTimeWatermark < mainEventTimeWatermark + interval 1 minute
      """.stripMargin))

    val query = stream.writeStream.foreach(RowProcessor).start()

    val sender = new OneKeyDataSender(query, mainEventsStream, joinedEventsStream)
    sender.thread.start()
    query.awaitTermination(120000)

    // As you can see, almost a half of rows weren't returned because of the range query
    // Normally we expect to have 45-50% of difference between sent and processed events
    val sentEventsKey1 = TestedValuesContainer.key1Counter.get()
    val processedEventsKey1 = TestedValuesContainer.values.groupBy(testedValues => testedValues.key)("key1").size
    val missingPercentage = processedEventsKey1.toDouble*100d / sentEventsKey1.toDouble
    val correctPercentage = missingPercentage >= 45 && missingPercentage <= 50
    correctPercentage shouldBe true
  }
  */