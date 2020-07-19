package com.cjw.kafkaproj.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Description
 * @Date 2020/7/18 21:03
 * @Created by cjw
 */
@RestController
@RequestMapping
public class KafkaController {

    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaController.class);

    private static final String topic = "topic_1";
    private static final String groupId = "group.demo";

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("/index")
    public String index(){
        return "hello";
    }

    /**
     * 生产者
     * @param input
     * @return
     */
    @GetMapping("/send/{input}")
    public String sendToKafka(@PathVariable("input") String input){
        kafkaTemplate.send(topic,input);
        return "send success "+input;
    }

    /**
     * 消费者
     * @param input
     */
    @KafkaListener(id = "myContainer1",topics = topic,groupId = groupId)
    public void listener(String input){
        LOGGER.info("接收到消息：{}",input);
    }
}
