package com.atguigu.flink.chapter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.util.*;

//自定义Sink MySQL
public class Demo27 {
    public static void main(String[] args) throws Exception {

        List<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromCollection(waterSensors).addSink(new RichSinkFunction<WaterSensor>() {

            private Connection connection;
            private PreparedStatement preparedStatement;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName("com.mysql.jdbc.Driver");//案例上没有写
                connection = DriverManager.getConnection("jdbc:mysql://192.168.10.102:3306/test?useSSL=false", "root", "123456");
                preparedStatement = connection.prepareStatement("insert into sensor values(?,?,?)");
            }

            @Override
            public void close() throws Exception {
                preparedStatement.close();
                connection.close();
            }

            @Override
            public void invoke(WaterSensor value, Context context) throws Exception {
                preparedStatement.setString(1, value.getId());
                preparedStatement.setLong(2, value.getTs());
                preparedStatement.setInt(3, value.getVc());
                preparedStatement.execute();
            }
        });
        executionEnvironment.execute();
    }
}
