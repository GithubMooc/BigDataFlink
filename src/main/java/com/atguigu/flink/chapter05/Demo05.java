package com.atguigu.flink.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

// 自定义Source
public class Demo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.addSource(new MySource("192.168.10.102",9999)).print();
        executionEnvironment.execute();
    }
    public static class MySource implements SourceFunction<WaterSensor>{

        private String host;
        private int port;
        private volatile boolean isRunning=true;
        private Socket socket;

        public MySource(String host,int port){
            this.host=host;
            this.port=port;
        }
        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            socket=new Socket(host,port);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String line=null;
            while(isRunning&&(line=bufferedReader.readLine())!=null){
                String[] split = line.split(",");
                sourceContext.collect(new WaterSensor(split[0],Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }

        }

        @Override
        public void cancel() {
            isRunning=false;
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
