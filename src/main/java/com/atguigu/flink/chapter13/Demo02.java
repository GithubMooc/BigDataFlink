package com.atguigu.flink.chapter13;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// 使用已注册的表
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);
//        使用sql查询一个已注册的表
//        1. 从流得到一个表

        Table table = streamTableEnvironment.fromDataStream(waterSensorDataStreamSource);
//        2. 把注册为一个临时视图
        streamTableEnvironment.createTemporaryView("sensor", table);
//        3. 在临时视图查询数据, 并得到一个新表
        Table resultTable = streamTableEnvironment.sqlQuery("select * from sensor where id='sensor_1'");
//        4. 显示resultTable的数据
        streamTableEnvironment.toAppendStream(resultTable, Row.class).print();
        executionEnvironment.execute();
    }
}
