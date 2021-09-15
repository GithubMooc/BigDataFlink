package com.atguigu.flink.chapter01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//无界流：WordCount lambda表达式
public class Demo06 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("localhost", 9999);
        dataStreamSource.flatMap((String s, Collector<Tuple2<String, Long>> collector) -> {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(Tuple2.of(s2, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG)).keyBy(t -> t.f0).sum(1).print();
        executionEnvironment.execute();
    }
}
