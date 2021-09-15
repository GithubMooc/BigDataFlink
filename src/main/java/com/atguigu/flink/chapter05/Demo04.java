package com.atguigu.flink.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

// 从kafka读取数据
// 开启kafka生产者,测试消费
// kafka-console-producer.sh --broker-list hadoop102:9092 --topic sensor
public class Demo04 {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.10.102:9092,192.168.10.103:9092,192.168.10.104:9092");
        properties.setProperty("group.id", "FlinkKafka");
        properties.setProperty("auto.offset.reset", "latest");
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties)).print();
        executionEnvironment.execute();
    }
}
