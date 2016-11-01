
object TestApp extends App {
  val obj = MyObj(Some("MY-NAME"),Some(MyInnerObj()), Seq(MyInnerObj(),MyInnerObj()))
  println(MyObjSerde.encode(obj).toList)
}