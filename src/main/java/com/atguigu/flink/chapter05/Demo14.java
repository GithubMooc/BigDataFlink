package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//      connect
//1.	两个流中存储的数据类型可以不同
//2.	只是机械的合并在一起, 内部仍然是分离的2个流
//3.	只能2个流进行connect, 不能有第3个参与

public class Demo14 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> integerDataStreamSource1 = executionEnvironment.fromElements(2, 4, 6, 8, 10);
        ConnectedStreams<Integer, Integer> connect = integerDataStreamSource.connect(integerDataStreamSource1);
        connect.getFirstInput().print();
        connect.getSecondInput().print();
        executionEnvironment.execute();
    }
}
