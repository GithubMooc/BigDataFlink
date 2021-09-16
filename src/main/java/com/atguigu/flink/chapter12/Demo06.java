package com.atguigu.flink.chapter12;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;

import static org.apache.flink.table.api.Expressions.*;


//kafka Sink
public class Demo06 {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        Table table = streamTableEnvironment.fromDataStream(waterSensorDataStreamSource);

        Table resultTable = table.where($("id").isEqual("sensor_1")).select($("id"), $("ts"), $("vc"));
        Schema schema = new Schema().field("id", DataTypes.STRING()).field("ts", DataTypes.BIGINT()).field("vc", DataTypes.INT());

        streamTableEnvironment.connect(new Kafka().version("universal").topic("sensor").sinkPartitionerRoundRobin().property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")).withFormat(new Json()).withSchema(schema).createTemporaryTable("sensor");


        resultTable.executeInsert("sensor");

        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
