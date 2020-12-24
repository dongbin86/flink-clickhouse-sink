## Flink clickhouse sink

* simple and efficient, at least once guarantee
* flink 1.8 is currently supported, and future versions are available for reference
* instead of using JDBC, use clickHouse's HTTP interface directly

### why I create this tool

At the beginning, I used this tool (https://github.com/ivi-ru/flink-clickhouse-sink), which linked to the official website,
but I found that it would cause data loss, and the flink slot could not be released normally when the clickHouse server showed abnormal response,
and the latest version also showed oom, so I rewrote this tool for people who want a simple clickhouse sink.

it has been well tested by chenglong.gu@perfma.com, have fun !

## Build 

mvn clean package


## usage

```
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.perfma.flink.clickhouse.ClickhouseSink
import com.perfma.flink.clickhouse.model.Consts
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @author hongshen
 * @since 2020/12/24
 */
object SaveToClickhouseJob {

  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)
    val topic = parameterTool.get("kafka.topic.name", "hongshen")
    val env = StreamExecutionEnvironment.createLocalEnvironment()

    val ckSinkerProps = new Properties
    ckSinkerProps.put(ClickhouseConstants.TARGET_TABLE_NAME, "db.table")
    ckSinkerProps.put(ClickhouseConstants.BATCH_SIZE, "20000")

    ckSinkerProps.put(ClickhouseConstants.INSTANCES, "localhost:8123")
    ckSinkerProps.put(ClickhouseConstants.USERNAME, "default")
    ckSinkerProps.put(ClickhouseConstants.PASSWORD, "")
    ckSinkerProps.put(ClickhouseConstants.FLUSH_INTERVAL, "2")

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "hongshen")

    val myConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), kafkaProps)

    myConsumer.setStartFromEarliest()

    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    val records = env.addSource(myConsumer).map(s => {
      val data = JSON.parseObject(s, classOf[Data])
      s"('${data.name}','${data.city}','${sdf.format(new Date(data.dateT))}','${data.ts}','${data.num}')"
    })

    records.addSink(new ClickhouseSink(ckSinkerProps)).setParallelism(2)

    env.execute("kafka2clickhouse")
  }
}
```
## Notice

The data format uses CSV format include '()' token on both side, and an INSERT statement is generated as follows

`String.format("INSERT INTO %s VALUES %s", tableName, csv)`

so you need convert your datastream event to that fomat, see the example above.

## Contributors

* hongshen(dong_bin86@163.com)
* chenglong(chenglong.gu@perfma.com)


## Sponsorship

![hongshen](https://github.com/dongbin86/flink-clickhouse-sink/blob/main/picture/hongshen.png)
![chenglong](https://github.com/dongbin86/flink-clickhouse-sink/blob/main/picture/chenglong.png)


Thank you for your sponsorship and support

