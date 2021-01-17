package com.study.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author zujs
 */
@Slf4j(topic = "KafkaProducerQuickStart")
public class KafkaProducerQuickStart {
    public static void main(String[] args) {

        //1.创建kafka consumer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS7A:9092,CentOS7B:9092,CentOS7C:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic01", "key" + i, "value" + i);
            //send
            producer.send(record);
        }
        //关闭生产者
        producer.close();

    }
}
