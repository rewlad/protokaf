
/*object TestApp extends App {
  //val obj = MyObj(Some("MY-NAME"),Some(MyInnerObj()), Seq(MyInnerObj(),MyInnerObj()))
  //println(MyObjProtoAdapter.encode(obj).toList)


}*/

//package test


import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, mapAsJavaMapConverter, seqAsJavaListConverter}

object Config {
  def getHost = Map[String,Object](
    "bootstrap.servers" → "localhost:9092"
  )
  def topic = "my-topic2"
}


object ProducerTest extends App {
  val props = Config.getHost ++ Map[String,String](
    "acks" → "all",
    "retries" → "0",
    "batch.size" → "16384",
    "linger.ms" → "1",
    "buffer.memory" → "33554432"
  )
  val serializer = new ByteArraySerializer
  val producer = new KafkaProducer[Array[Byte],Array[Byte]](props.asJava, serializer, serializer)

  val buffer = new okio.Buffer
  val writer = new com.squareup.wire.ProtoWriter(buffer)

  val adapterToId = ProtoAdapters.list.map(adapter ⇒ adapter→adapter.id).toMap
  
  //5000000 ~ 1/2 min
  (1 to 500000).foreach { i ⇒
    val obj = MyObj(Some(java.time.Instant.now()), List(
      MyInnerObj(Some("test"), Some(BigDecimal(i))),
      MyInnerObj(Some("test"), Some(BigDecimal(i+1)))
    ))
    val typeId: Int = adapterToId(MyObjProtoAdapter)
    KeyProtoAdapter.encode(writer, Key(Some(typeId)))
    //com.squareup.wire.ProtoAdapter.SINT32.encode(writer,typeId)
    val key = buffer.readByteArray()
    //println(key.toList)
    MyObjProtoAdapter.encode(writer, obj)
    val value = buffer.readByteArray()
    producer.send(new ProducerRecord(Config.topic, key, value))
    if(i % 100000 == 0) println(i)
  }
  producer.close()
}

object ConsumerTest extends App {
  val props = Config.getHost ++ Seq[(String,String)](
    "group.id" → "test",
    "enable.auto.commit" → "false"
  )
  val deserializer = new ByteArrayDeserializer
  val consumer = new KafkaConsumer[Array[Byte],Array[Byte]](props.asJava, deserializer, deserializer)
  //consumer.subscribe(List(Config.topic).asJava)
  val topicPartition = new TopicPartition(Config.topic,0)
  consumer.assign(List(topicPartition).asJava)
  consumer.seek(topicPartition, 0)
  def out(value: Object) = () //println(value)
  
  //val buffer = new okio.Buffer()
  //val reader = new com.squareup.wire.ProtoReader(buffer)
  

  val idToAdapter = ProtoAdapters.list.map(adapter ⇒ adapter.id→adapter).toMap
  
  val start = System.currentTimeMillis
  var i = 0
  while(true) { // 5M msg: kafka 10s, kafka+scala_pb+case 13s,
    //println("p")
    consumer.poll(1000).asScala.foreach { rec ⇒
      //println("k",rec.key.toList)
      //buffer.write(rec.key)
      //val typeId = KeyProtoAdapter.decode(reader).key.get
      val typeId = KeyProtoAdapter.decode(rec.key).key.get

      //val typeId = com.squareup.wire.ProtoAdapter.SINT32.decode(reader)
      //println("t",typeId)
      val adapter = idToAdapter(typeId)
      //buffer.write(rec.value)
      //val obj = adapter.decode(reader)
      val obj = adapter.decode(rec.value)
      out(obj)
      i += 1
      if(i % 100000 == 0) println(i,System.currentTimeMillis-start)
    }
  }
}

/*
Decimal ⇒ {
  resultType ⇒ "BigDecimal"
  "get scale" ⇒ "value.bigDecimal.scale" 0
  "get bytes" ⇒ "new okio.ByteString(value.bigDecimal.unscaledValue.toByteArray)" okio.ByteString.EMPTY
}
Instant {
  resultType java.time.Instant

}
"string" {
  defaultValue ""
}
"sint32" {
  defaultValue 0
}
"sint64" {
  defaultValue 0
}
bytes {
  defaultValue okio.ByteString.EMPTY
}
*/

object DecimalProtoAdapter extends com.squareup.wire.ProtoAdapter[BigDecimal](com.squareup.wire.FieldEncoding.LENGTH_DELIMITED, classOf[BigDecimal]) {
  override def encodedSize(value: BigDecimal): Int = {
    var res = 0
    res += com.squareup.wire.ProtoAdapter.SINT32.encodedSizeWithTag(0x0001, value.bigDecimal.scale)
    val bytes = value.bigDecimal.unscaledValue.toByteArray
    res += com.squareup.wire.ProtoAdapter.BYTES.encodedSizeWithTag(0x0002, okio.ByteString.of(bytes,0,bytes.length))
    res
  }
  override def encode(writer: com.squareup.wire.ProtoWriter, value: BigDecimal) = {
    com.squareup.wire.ProtoAdapter.SINT32.encodeWithTag(writer, 0x0001, value.bigDecimal.scale)
    val bytes = value.bigDecimal.unscaledValue.toByteArray
    com.squareup.wire.ProtoAdapter.BYTES.encodeWithTag(writer, 0x0002, okio.ByteString.of(bytes,0,bytes.length))
  }
  override def decode(reader: com.squareup.wire.ProtoReader) = {
    var scale: Int = 0
    var bytes: okio.ByteString = okio.ByteString.EMPTY
    var done = false
    val token = reader.beginMessage()
    while(!done) reader.nextTag() match {
      case -1 => done = true
      case 0x0001 ⇒ scale = com.squareup.wire.ProtoAdapter.SINT32.decode(reader)
      case 0x0002 ⇒ bytes = com.squareup.wire.ProtoAdapter.BYTES.decode(reader)
      case _ => reader.peekFieldEncoding.rawProtoAdapter.decode(reader)
    }
    reader.endMessage(token)
    Decimal(scale, bytes)
  }
}

object Decimal {
  def apply(scale: Int, bytes: okio.ByteString): BigDecimal =
    BigDecimal(new java.math.BigDecimal(new java.math.BigInteger(bytes.toByteArray), scale))
}


object InstantProtoAdapter extends com.squareup.wire.ProtoAdapter[java.time.Instant](com.squareup.wire.FieldEncoding.LENGTH_DELIMITED, classOf[java.time.Instant]) {
  override def encodedSize(value: java.time.Instant): Int = {
    var res = 0
    res += com.squareup.wire.ProtoAdapter.SINT64.encodedSizeWithTag(0x0001, value.getEpochSecond)
    res += com.squareup.wire.ProtoAdapter.SINT32.encodedSizeWithTag(0x0002, value.getNano)
    res
  }
  override def encode(writer: com.squareup.wire.ProtoWriter, value: java.time.Instant) = {
    com.squareup.wire.ProtoAdapter.SINT64.encodeWithTag(writer, 0x0001, value.getEpochSecond)
    com.squareup.wire.ProtoAdapter.SINT32.encodeWithTag(writer, 0x0002, value.getNano)
  }
  override def decode(reader: com.squareup.wire.ProtoReader) = {
    var epochSecond: Long = 0
    var nanoOfSecond: Int = 0
    var done = false
    val token = reader.beginMessage()
    while(!done) reader.nextTag() match {
      case -1 => done = true
      case 0x0001 ⇒ epochSecond = com.squareup.wire.ProtoAdapter.SINT64.decode(reader)
      case 0x0002 ⇒ nanoOfSecond = com.squareup.wire.ProtoAdapter.SINT32.decode(reader)
      case _ => reader.peekFieldEncoding.rawProtoAdapter.decode(reader)
    }
    reader.endMessage(token)
    Instant(epochSecond, nanoOfSecond)
  }
}

object Instant {
  def apply(epochSecond: Long, nanoOfSecond: Int) =
    java.time.Instant.ofEpochSecond(epochSecond, nanoOfSecond)
}

/*
object ToBytes {
  def apply(value: Object) = value match {
    case v: Instant ⇒
      val w = Test.Instant.newBuilder().setSeconds(v.getEpochSecond).setNanos(v.getNano).build()
      inner("0f2155cb-684b-482e-a127-ab1fc8389137", w.toByteArray)
    case v: BigDecimal ⇒
      inner("cbaf5b18-2fc0-40e0-9d6a-8ee07d5c2924", v.toString.getBytes("UTF-8"))
  }
  def inner(typeName: String, value: Array[Byte]) = {
    Test.Unknown.newBuilder().setDataType(typeName).setData(com.google.protobuf.ByteString.copyFrom(value)).build().toByteArray
  }
}

object FromBytes {
  def apply(bytes: Array[Byte]): Object = {
    val u = Test.Unknown.parseFrom(bytes)
    u.getDataType match {
      case "0f2155cb-684b-482e-a127-ab1fc8389137" ⇒
        val t = Test.Instant.parseFrom(u.getData.toByteArray)
        Instant.ofEpochSecond(t.getSeconds, t.getNanos)
      case "cbaf5b18-2fc0-40e0-9d6a-8ee07d5c2924" ⇒
        val t = new String(u.getData.toByteArray, "UTF-8")
        t
      //BigDecimal(t)
    }
  }
}



*/