package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 滚动聚合算子 sum max min
// 滚动聚合算子： 来一条，聚合一条
//        1、聚合算子在 keyBy之后调用，因为这些算子都是属于 KeyedStream 里的
//        2、聚合算子，作用范围，都是分组内。 也就是说，不同分组，要分开算。
//        3、max、maxBy的区别：
//            max：取指定字段的当前的最大值，如果有多个字段，其他非比较字段，以第一条为准
//            maxBy：取指定字段的当前的最大值，如果有多个字段，其他字段以最大值那条数据为准；
//            如果出现两条数据都是最大值，由第二个参数决定： true => 其他字段取 比较早的值； false => 其他字段，取最新的值
public class Demo16 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4, 5);
        KeyedStream<Integer, String> integerStringKeyedStream = integerDataStreamSource.keyBy(r -> r % 2 == 0 ? "偶数" : "奇数");
        integerStringKeyedStream.sum(0).print();
        integerStringKeyedStream.max(0).print();
        integerStringKeyedStream.min(0).print();
        executionEnvironment.execute();
    }
}
