package com.zgw.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by Zhaogw&Lss on 2019/11/18.
 * Kafka生产者
 */
public class KafkaProducer extends Thread{
    public String topic;
    private Producer<Integer,String> producer;

    public KafkaProducer(String topic){
        this.topic = topic;
        Properties properties = new Properties();

        properties.put("metadata.broker.list",KafkaProperties.Broker_List);

        //设置序列化
        properties.put("serializer.class","kafka.serializer.StringEncoder");

        properties.put("request.required.acks","1");


        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

        @Override
        public void run(){
        int messageNo = 1;
        while (true){
          String  message = "message"+messageNo;
          producer.send(new KeyedMessage<Integer, String>(topic,message));

            System.out.println("Sent "+message);
            messageNo++;

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        }

}
