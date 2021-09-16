package com.atguigu.flink.chapter14;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

//        1、创建表的执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

//        2、创建表：将流转换成动态表，表的字段名从pojo的属性名自动获取
        Table table = streamTableEnvironment.fromDataStream(waterSensorDataStreamSource,$("id"), $("ts"), $("vc"), $("pt").proctime());

//
        table.execute().print();
    }
}
