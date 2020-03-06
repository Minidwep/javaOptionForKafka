package com.kafka.kafka.producer;


import org.apache.kafka.clients.producer.*;
import scala.collection.mutable.ArrayLike;

import java.util.ArrayList;
import java.util.Properties;

public class CustomerProducer {
    public static void main(String[] args) {

//        配置信息
        Properties props = new Properties();
//        kafka集群
        props.put("bootstrap.servers", "39.106.72.218:9092");
//        应答级别 all
        props.put("acks", "all");
//        重试次数
        props.put("retries", 0);
//        批量大小
        props.put("batch.size", 16384);
//        提交延时
        props.put("linger.ms", 1);
//        时间到1s或者大小为 16384 发送

//        缓存
        props.put("buffer.memory", 33554432);
//        序列化的类 KV的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        设置分区
//        props.put("partitioner.class", "com.kafka.kafka.producer.CustomerPartitioner");

        ArrayList list = new ArrayList<String>();
//        谁先添加谁先执行
        list.add("com.kafka.kafka.intercetor.CountIntercetor");
        list.add("com.kafka.kafka.intercetor.TimeIntercetor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list );
//        创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

//        循环发送数据partitioner.class
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", String.valueOf(i)), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(metadata.partition()+"------"+metadata.offset());
                } else {
                    System.out.println("发送失败");
                }
            });
        }
//        关闭资源
        producer.close();

    }
}
