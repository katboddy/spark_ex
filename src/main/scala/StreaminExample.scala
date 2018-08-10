import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kinesis._

object StreamingExample {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[2]").setAppName("testApp")
    val batchInterval = Seconds(10)
    val ssc = new StreamingContext(config, batchInterval)

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint("kinesis.us-west-2.amazonaws.com")

    val numShards = kinesisClient.describeStream("test-spark").getStreamDescription().getShards.size()


    val kinesisStreams = (0 until numShards).map { i =>
      KinesisInputDStream.builder.streamingContext(ssc)
        .endpointUrl("kinesis.us-west-2.amazonaws.com")
        .regionName("us-west-2")
        .streamName("test-spark")
        .checkpointAppName("testApp")
        .checkpointInterval(batchInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    val streamUnion = ssc.union(kinesisStreams)

    val words = streamUnion.flatMap(byteArray => new String(byteArray).split(" "))

    // Map each word to a (word, 1) tuple so we can reduce by key to count the words
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)

    // Print the first 10 wordCounts
    wordCounts.print()


    val record = streamUnion.flatMap(byteArray => new String(byteArray))
    println(record)

    val hashTags = words.filter{word => word.startsWith("#")}
    val hashTagPairs = hashTags.map{hashtag => (hashtag, 1)}
    val tagsWithCounts = hashTagPairs.updateStateByKey((counts: Seq[Int], prevCount: Option[Int]) =>
      prevCount.map{c  => c + counts.sum}.orElse{Some(counts.sum)})

    val dates = words.filter{word => word.startsWith("2018")}







    ssc.start()
    ssc.awaitTermination()

  }

}
