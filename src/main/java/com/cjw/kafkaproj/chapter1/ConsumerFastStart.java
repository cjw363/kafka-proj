package com.cjw.kafkaproj.chapter1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Classname ConsumerFastStart
 * @Description
 * @Date 2020/7/18 14:50
 * @Created by cjw
 */
public class ConsumerFastStart {
    private static final String brokerList = "152.136.134.235:9092";
    private static final String topic = "topic_1";
    private static final String groupId = "group.demo";


    public static void main(String[] args) {
        Properties properties = new Properties();
        //        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //        properties.put("bootstrap.servers", brokerList);
        //        properties.put("group.id",groupId);

        //设置key反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置值反序列化器
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置集群地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        //消费组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record:records){
                System.out.println(record.value());
            }
        }

    }
}
