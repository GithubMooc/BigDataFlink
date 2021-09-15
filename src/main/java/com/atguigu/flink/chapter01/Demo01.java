package com.atguigu.flink.chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//批处理WordCount：匿名内部类
public class Demo01 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDS = env.readTextFile("input/word.txt");
        FlatMapOperator<String, Tuple2<String, Long>> stringTuple2FlatMapOperator = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(Tuple2.of(s1, 1L));
                }
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);

        sum.print();

    }
}
