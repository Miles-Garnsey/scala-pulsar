package scala.pulsar

import com.sksamuel.pulsar4s._
import com.sksamuel.avro4s._
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.impl.schema.AvroSchema
import org.apache.pulsar.common.schema.KeyValue

object App {
  def main(args: Array[String]): Unit = {
    printPulsar()
  }

  def printPulsar(): Unit = {
    val client = PulsarClient("pulsar://localhost:62775")
    val topic = Topic("persistent://public/default/data-db1.table1")
    implicit val schema: Schema[KeyValue[db1.table1key, db1.table1value]] =
      Schema.KeyValue(Schema.AVRO(classOf[db1.table1key]), Schema.AVRO(classOf[db1.table1value]))
    val consumer = client.consumer(ConsumerConfig(Subscription("mysubs"), List(topic)))
    consumer.seek(MessageId.earliest)
    while (true) {
      val message = consumer.receive
      println(message.toString)
    }
  }
}