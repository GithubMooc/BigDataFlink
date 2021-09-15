package com.atguigu.flink.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//富函数
public class Demo09 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromElements(1, 2, 3, 4, 5).map(new MyRichMapFunction()).setParallelism(2).print();
        executionEnvironment.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<Integer, Integer> {

        //默认生命周期初始方法，在每个并行度上只会被调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open 执行一次");
        }

        //默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次
        @Override
        public void close() throws Exception {
            System.out.println("close方法执行一次");
        }


        @Override
        public Integer map(Integer integer) throws Exception {
            System.out.println("map方法执行");
            return integer * integer;
        }
    }
}
