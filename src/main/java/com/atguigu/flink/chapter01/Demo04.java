package com.atguigu.flink.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//有界流，lambda表达式
public class Demo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);


        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("input/word.txt");
        stringDataStreamSource.flatMap((String s, Collector<String> collector) -> {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
        }).returns(Types.STRING).map(s -> Tuple2.of(s, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG)).keyBy(t -> t.f0).sum(1).print();
        executionEnvironment.execute();
    }
}
