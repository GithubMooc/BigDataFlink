package com.atguigu.flink.chapter13;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Master
 */

// 使用未注册的表
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        //使用未注册的表
        Table table = streamTableEnvironment.fromDataStream(waterSensorDataStreamSource);

        Table resultTable = streamTableEnvironment.sqlQuery("select * from " + table + " where id='sensor_1'");
        streamTableEnvironment.toAppendStream(resultTable, Row.class).print();

        executionEnvironment.execute();


    }
}
