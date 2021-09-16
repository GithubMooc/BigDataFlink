package com.atguigu.flink.chapter12;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

//通过文件读取 File Source
public class Demo03 {
    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        //获取表的执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(executionEnvironment);

        Schema schema = new Schema().field("id", "String").field("ts", "BIGINT").field("vc", "Integer");

        streamTableEnvironment.connect(new FileSystem().path("input/sensor.txt")).withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n")).withSchema(schema).createTemporaryTable("sensor");

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
