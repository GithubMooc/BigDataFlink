package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//keyBy lanbda
public class Demo12 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(1,2,3,4,5).keyBy(i->i%2==0?"偶数":"奇数").print();
        executionEnvironment.execute();
    }
}
