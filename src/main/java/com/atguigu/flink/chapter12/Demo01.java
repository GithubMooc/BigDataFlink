package com.atguigu.flink.chapter12;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

//2.2	基本使用:表与DataStream的混合使用
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
        Table table = streamTableEnvironment.fromDataStream(waterSensorDataStreamSource);

//         3. 对动态表进行查询
        Table resultTable = table.where($("id").isEqual("sensor_1")).select($("id"), $("ts"), $("vc"));

//        4、把动态表转换成流
        DataStream<Row> rowDataStream = streamTableEnvironment.toAppendStream(resultTable, Row.class);
        rowDataStream.print();
        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
