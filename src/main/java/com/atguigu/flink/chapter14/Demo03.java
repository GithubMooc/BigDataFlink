package com.atguigu.flink.chapter14;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = executionEnvironment.fromElements(new WaterSensor("sensor_1", 1000L, 10), new WaterSensor("sensor_1", 2000L, 20), new WaterSensor("sensor_2", 3000L, 30), new WaterSensor("sensor_1", 4000L, 40), new WaterSensor("sensor_1", 5000L, 50), new WaterSensor("sensor_2", 6000L, 60)).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        Table table = streamTableEnvironment.fromDataStream(waterSensorSingleOutputStreamOperator, $("id"), $("ts"), $("vc"), $("et").rowtime());
        table.execute().print();
        executionEnvironment.execute();
    }
}
