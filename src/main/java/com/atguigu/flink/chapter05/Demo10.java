package com.atguigu.flink.chapter05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// flatMap
public class Demo10 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(1, 2, 3, 4, 5).flatMap((Integer integer, Collector<Integer> collector) -> {
            collector.collect(integer * integer);
            collector.collect(integer * integer * integer);
        }).returns(Types.INT).print();
        executionEnvironment.execute();
    }
}
