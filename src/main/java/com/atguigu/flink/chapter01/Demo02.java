package com.atguigu.flink.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//批处理：lambda表达式
public class Demo02 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = env.readTextFile("input/word.txt");
        FlatMapOperator<String, Tuple2<String, Long>> returns = stringDataSource.flatMap((String s, Collector<Tuple2<String, Long>> out) -> {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                out.collect(Tuple2.of(s2, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = returns.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);
        sum.print();
    }
}
