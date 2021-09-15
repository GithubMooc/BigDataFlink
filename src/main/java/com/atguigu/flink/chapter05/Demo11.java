package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//filter
public class Demo11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(1, 2, 3, 4, 5).filter(i -> i % 2 == 0).print();
        executionEnvironment.execute();
    }
}
