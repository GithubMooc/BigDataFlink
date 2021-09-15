package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//从文件中读取数据
//1.	参数可以是目录也可以是文件
//2.	路径可以是相对路径也可以是绝对路径
//3.	相对路径是从系统属性user.dir获取路径: idea下是project的根目录, standalone模式下是集群节点根目录
//4.	也可以从hdfs目录下读取, 使用路径:hdfs://hadoop102:8020/...., 由于Flink没有提供hadoop相关依赖, 需要pom中添加相关依赖:
//      hadoop-client
public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.readTextFile("input/word.txt").print();
        executionEnvironment.execute();
    }
}
