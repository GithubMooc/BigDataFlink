package com.atguigu.flink.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo13 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(1,2,3,4,5).keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer integer) throws Exception {
                return integer%2==0?"偶数":"奇数";
            }
        }).print();
        executionEnvironment.execute();
    }
}
