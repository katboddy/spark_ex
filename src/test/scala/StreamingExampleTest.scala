package org.apache.spark.streaming.kinesis

import org.apache.spark.SparkConf
import org.scalatest.mockito.MockitoSugar
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.Latest
import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterEach
import KinesisInputDStream._

class StreaminExampleTest extends SparkFunSuite with BeforeAndAfterEach with MockitoSugar {

  val config = new SparkConf().setMaster("local[2]").setAppName("testApp")
  val batchInterval = Seconds(10)
  val ssc = new StreamingContext(config, batchInterval)
  val streamName = "a-very-nice-kinesis-stream-name"
  val checkpointAppName = "a-very-nice-kcl-app-name"
  def baseBuilder = KinesisInputDStream.builder
  def builder = baseBuilder.streamingContext(ssc)
    .streamName(streamName)
    .checkpointAppName(checkpointAppName)

  override def afterAll(): Unit = {
    ssc.stop()
  }

  test("should raise an exception if the StreamingContext is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamName(streamName).checkpointAppName(checkpointAppName).build()
    }
  }

  test("should raise an exception if the stream name is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamingContext(ssc).checkpointAppName(checkpointAppName).build()
    }
  }

  test("should raise an exception if the checkpoint app name is missing") {
    intercept[IllegalArgumentException] {
      baseBuilder.streamingContext(ssc).streamName(streamName).build()
    }
  }

  test("should propagate required values to KinesisInputDStream") {
    val dstream = builder.build()
    assert(dstream.context == ssc)
    assert(dstream.streamName == streamName)
    assert(dstream.checkpointAppName == checkpointAppName)
  }

  test("should propagate default values to KinesisInputDStream") {
    val dstream = builder.build()
    assert(dstream.endpointUrl == DEFAULT_KINESIS_ENDPOINT_URL)
    assert(dstream.regionName == DEFAULT_KINESIS_REGION_NAME)
    assert(dstream.initialPosition == DEFAULT_INITIAL_POSITION)
    assert(dstream.checkpointInterval == batchInterval)
    assert(dstream._storageLevel == DEFAULT_STORAGE_LEVEL)
    assert(dstream.kinesisCreds == DefaultCredentials)
    assert(dstream.dynamoDBCreds == None)
    assert(dstream.cloudWatchCreds == None)
  }

  test("should propagate custom non-auth values to KinesisInputDStream") {
    val customEndpointUrl = "https://kinesis.us-west-2.amazonaws.com"
    val customRegion = "us-west-2"
    val customInitialPosition = new Latest()
    val customAppName = "a-very-nice-kinesis-app"
    val customCheckpointInterval = Seconds(30)
    val customStorageLevel = StorageLevel.MEMORY_ONLY

    val dstream = builder
      .endpointUrl(customEndpointUrl)
      .regionName(customRegion)
      .initialPosition(customInitialPosition)
      .checkpointAppName(customAppName)
      .checkpointInterval(customCheckpointInterval)
      .storageLevel(customStorageLevel)
      .build()
    assert(dstream.endpointUrl == customEndpointUrl)
    assert(dstream.regionName == customRegion)
    assert(dstream.initialPosition == customInitialPosition)
    assert(dstream.checkpointAppName == customAppName)
    assert(dstream.checkpointInterval == customCheckpointInterval)
    assert(dstream._storageLevel == customStorageLevel)
  }

}
