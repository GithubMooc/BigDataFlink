package com.atguigu.flink.chapter12;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

//通过kafka读取数据
public class Demo04 {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        Schema schema = new Schema().field("id", DataTypes.STRING()).field("ts", DataTypes.BIGINT()).field("vc", DataTypes.INT());

        streamTableEnvironment.connect(new Kafka().version("universal").topic("sensor").startFromLatest().property("group.id", "bigdata").property("bootstrap.servers", "192.168.10.102:9092,192.168.10.103:9092,192.168.10.104:9092")).withFormat(new Json()).withSchema(schema).createTemporaryTable("sensor");

        Table sensor = streamTableEnvironment.from("sensor");
        Table resultTable = sensor.groupBy($("id")).select($("id"), $("id").count().as("cnt"));

        DataStream<Tuple2<Boolean, Row>> resultStream = streamTableEnvironment.toRetractStream(resultTable, Row.class);

        resultStream.print();

        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
