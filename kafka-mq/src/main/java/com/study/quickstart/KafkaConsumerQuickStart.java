package com.study.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zujs
 */
@Slf4j(topic = "KafkaConsumerQuickStart")
public class KafkaConsumerQuickStart {
    public static void main(String[] args) {

        //创建kafka producer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS7A:9092,CentOS7B:9092,CentOS7C:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g2");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //2. 订阅相关topic
        consumer.subscribe(Arrays.asList("topic01"));

        //3. 遍历消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();

                    String key = record.key();
                    String value = record.value();
                    long timestamp = record.timestamp();

                    log.info(topic + ",partition:" + partition + ",offset:" + offset + "," + key + "," + value);
                }
            }
        }

    }
}
