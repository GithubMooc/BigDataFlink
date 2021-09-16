package com.atguigu.flink.chapter14;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo02 {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.fromElements();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        streamTableEnvironment.executeSql("create table sensor(id string,ts bigint,vc int,pt_time as PROCTIME()) with" +
                "('connector' = 'filesystem','path' = 'input/sensor.txt','format' = 'csv')");
        TableResult result = streamTableEnvironment.executeSql("select * from sensor");
        result.print();
    }
}
