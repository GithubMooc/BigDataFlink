package com.atguigu.flink.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Transform：map
// 静态内部类
public class Demo08 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(1, 2, 3, 4, 5).map(new MyMapFunction()).print();
        executionEnvironment.execute();
    }

    public static class MyMapFunction implements MapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer integer) throws Exception {
            return integer * integer;
        }
    }
}
