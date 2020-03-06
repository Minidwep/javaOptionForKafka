package com.kafka.kafka.consumer;

import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据指定的 topic partition offset 来获取数据
 */
public class LowerConsumer {
    public static void main(String[] args) {

//        定义参数
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("39.106.72.218");
        brokers.add("47.113.113.196");
        brokers.add("39.96.72.169");

//        端口号
        int port = 9092;
//        主题
        String topic = "first";
//        分区
        int partition = 0;
//        offset
        long offset = 2;

        LowerConsumer lowerConsumer = new LowerConsumer();

        lowerConsumer.getData(brokers, port, topic,partition,offset);

    }

    //    寻找分区leader
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition) {
        for (String broker : brokers) {
//            创建获取分区leader的消费者对象（获得 master slave01 slave02的leader）
            SimpleConsumer getLeader = new SimpleConsumer(broker, port, 1000, 1024 * 4, "getLeader");

//            创建主题元数据请求信息（创建first[topic]数据请求信息元数据）
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));

//            获取主题元数据返回值（获取first[topic]元数据返回值）
            TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);

//            解析元数据返回值（获取first[topic]元数据返回值）
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();

//            遍历主题元数据（遍历first[topic]元数据 实际执行一次，因为只传入了一个topic）
            for (TopicMetadata topicMetadata : topicsMetadata) {
//                获取多个分区的元数据信息
                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
//                遍历分区元数据信息（如果分区有三个[0,1,2]，进行遍历）
                for (PartitionMetadata partitionMetadata : partitionsMetadata) {
//                    找到需要传入的分区
                    if(partition==partitionMetadata.partitionId()){
//                       返回分区所在的Broker中的leader（因为有多个副本，所以要找到副本中的leader 并返回）
                    return  partitionMetadata.leader();
                    }

                }
            }


        }

        return null;
    }

    //    获取数据
    private void getData(List<String> brokers, int port, String topic, int partition,long offset) {

        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if(leader == null){
            System.out.println("leader为空，请检测节点kafka是否启动成功");
            return;
        }
        String leaderHost = leader.host();

//        获取数据的消费者对象
        SimpleConsumer getData = new SimpleConsumer(leaderHost, port, 1000, 1024 * 4, "getData");

//        获取数据的对象
        kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000000).build();

//        获取数据返回值
        FetchResponse fetchResponse = getData.fetch(fetchRequest);

//        解析返回值
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
//        遍历并打印
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1+"----"+new String(bytes));
        }
    }
}
