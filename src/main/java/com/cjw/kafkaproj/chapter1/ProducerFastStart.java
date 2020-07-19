package com.cjw.kafkaproj.chapter1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Classname ProducerFastStart
 * @Description
 * @Date 2020/7/18 14:13
 * @Created by cjw
 */
public class ProducerFastStart {

    private static final String brokerList = "152.136.134.235:9092";
    private static final String topic = "topic_1";

    public static void main(String[] args) {
        Properties properties = new Properties();
        //        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //        properties.put("bootstrap.servers", brokerList);

        //设置key序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        //设置值序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //设置集群地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,"kafka-demo-key","hello kafka3");
        try {
            producer.send(record);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
