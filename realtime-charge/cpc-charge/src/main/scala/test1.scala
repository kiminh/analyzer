import com.cpc.spark.Data2Kafka

object test1 {
  def main(args: Array[String]): Unit = {
    val brokers = "192.168.80.35:9092,192.168.80.36:9092,192.168.80.37:9092,192.168.80.88:9092,192.168.80.89:9092"
    val ts = System.currentTimeMillis()
    val toKafka = new Data2Kafka()
    toKafka.setMessage(ts, mapInt = Seq(("set_charge_offset_failed", 1)))
    toKafka.sendMessage(brokers, "test")
    toKafka.close()
  }
}
