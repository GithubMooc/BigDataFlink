package com.atguigu.flink.chapter13;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//kafka到kafka
public class Demo04 {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        // 1. 注册SourceTable: source_sensor
        streamTableEnvironment.executeSql("create table source_sensor (id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_source_sensor',"
                + "'properties.bootstrap.servers' = '192.168.10.102:9092,192.168.10.103:9092,192.168.10.104:9092',"
                + "'properties.group.id' = 'atguigu',"
                + "'scan.startup.mode' = 'latest-offset',"
                + "'format' = 'csv'"
                + ")"
        );

        // 2. 注册SinkTable: sink_sensor
        streamTableEnvironment.executeSql("create table sink_sensor(id string, ts bigint, vc int) with("
                + "'connector' = 'kafka',"
                + "'topic' = 'topic_sink_sensor',"
                + "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'format' = 'csv'"
                + ")"
        );
        // 3. 从SourceTable 查询数据, 并写入到 SinkTable
        streamTableEnvironment.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");
    }
}
