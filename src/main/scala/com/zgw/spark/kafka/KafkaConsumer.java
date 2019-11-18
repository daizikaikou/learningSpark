package com.zgw.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Zhaogw&Lss on 2019/11/18.
 */
public class KafkaConsumer extends Thread {
    public String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;

    }
    private ConsumerConnector createConnector(){
        Properties properties = new Properties();

        properties.put("zookeeper.connect",KafkaProperties.ZK);

        properties.put("group.id",KafkaProperties.GROUP_ID);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }
    @Override
    public void run(){
        ConsumerConnector consumer =  createConnector();
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic,1);
        //String topic
        //List<KafkaStream<byte[], byte[]>>>  对应的数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  messageStreams.get(topic).get(0);   //获取每次接收到的数据

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            while (iterator.hasNext()){
               String message =  new String(iterator.next().message());
                System.out.println("recive" + message);
            }
    }
}
