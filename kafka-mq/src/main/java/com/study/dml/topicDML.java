package com.study.dml;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author zujs
 */
@Slf4j(topic = "topicDML")
public class topicDML {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //创建KafkaAdminClient
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "CentOS7A:9092,CentOS7B:9092,CentOS7C:9092");

        KafkaAdminClient adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);

//        //创建topic， 默认异步创建
//        CreateTopicsResult createTopics = adminClient.createTopics(Arrays.asList(new NewTopic("topic06", 3, (short) 3)));
//        //同步创建
//        createTopics.all().get();

//        //删除topic
//        DeleteTopicsResult deleteTopics = adminClient.deleteTopics(Arrays.asList("topic05", "topic06"));
//        //同步删除
//        deleteTopics.all().get();

        //查看topic列表
        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> topics = topicsResult.names().get();
        for (String topic : topics) {
            log.info(topic);
        }

        //查看topic详情
        DescribeTopicsResult describeTopics = adminClient.describeTopics(Arrays.asList("topic02"));
        Map<String, TopicDescription> topicDescriptionMap = describeTopics.all().get();
        topicDescriptionMap.forEach((key, value) -> log.info(key + value));


        //关闭创建KafkaAdminClient
        adminClient.close();
    }
}
