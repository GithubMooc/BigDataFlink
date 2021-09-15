package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//      union
//1.	union之前两个流的类型必须是一样，connect可以不一样
//2.	connect只能操作两个流，union可以操作多个。

public class Demo15 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 4, 7, 10);
        DataStreamSource<Integer> integerDataStreamSource1 = executionEnvironment.fromElements(2, 5, 8, 11);
        DataStreamSource<Integer> integerDataStreamSource2 = executionEnvironment.fromElements(3, 6, 9, 12);
        DataStream<Integer> union = integerDataStreamSource2.union(integerDataStreamSource1).union(integerDataStreamSource);
        union.print();
        executionEnvironment.execute();
    }
}
