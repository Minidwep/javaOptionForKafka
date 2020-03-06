package com.kafka.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {
    public static void main(String[] args) {
//        配置信息
        Properties props = new Properties();
//        kafka集群
        props.put("bootstrap.servers", "39.106.72.218:9092");
//        消费者组id
        props.put("group.id", "test1");
//      当时一个新消费者组id，且没有offset以后，才启用该下配置。从最早的数据开始加载。
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        是否自动提交 offset
        props.put("enable.auto.commit", "true");
//        自动提交延时
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        创建消费者对象
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer(props);
//        指定tipoc
        kafkaConsumer.subscribe(Arrays.asList("first"));

//        重新消费信息
//        kafkaConsumer.assign(Collections.singletonList(new TopicPartition("first", 0)));
//
//        kafkaConsumer.seek(new TopicPartition("first", 0),50);

        while (true){
//            拉取数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String,String> record : records){
                System.out.println(record.topic()+"--------"
                        + record.partition()
                        +"--------"+record.value()+"-----"+record.offset());
            }
        }


    }
}
