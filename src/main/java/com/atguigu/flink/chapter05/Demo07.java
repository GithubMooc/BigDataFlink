package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Transform：map
// lambda表达式
public class Demo07 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(1, 2, 3, 4, 5).map(i -> i * i).print();
        executionEnvironment.execute();
    }
}
