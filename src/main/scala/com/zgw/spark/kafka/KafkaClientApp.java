package com.zgw.spark.kafka;

/**
 * Created by Zhaogw&Lss on 2019/11/18.
 * kafka javaApi
 */
public class KafkaClientApp {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.Topic).start();
        new KafkaConsumer(KafkaProperties.Topic).start();
    }
}
