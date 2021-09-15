package com.atguigu.flink.chapter05;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.*;

import java.util.*;

//Sink到Redis集群
public class Demo25 {
    public static void main(String[] args) throws Exception {

        List<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        //连接到Redis的配置
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder().setHost("192.168.10.102").setPort(6379).setMaxTotal(100).setTimeout(1000 * 10).build();

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.fromCollection(waterSensors).addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {

            @Override
            public RedisCommandDescription getCommandDescription() {
                // 返回存在Redis中的数据类型  存储的是Hash, 第二个参数是外面的key
                return new RedisCommandDescription(RedisCommand.HSET,"sensor");
            }

            @Override
            public String getKeyFromData(WaterSensor waterSensor) {
                // 从数据中获取Key: Hash的Key
                return waterSensor.getId();
            }

            @Override
            public String getValueFromData(WaterSensor waterSensor) {
                // 从数据中获取Value: Hash的value
                return JSON.toJSONString(waterSensor);
            }
        }));
        executionEnvironment.execute();
    }
}
