package com.demo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Kafka Topic 配置类
 *
 * <p>SpringBoot3 会自动装配 KafkaAdmin：项目启动时检测到 NewTopic 类型的 Bean，
 * 会自动创建不存在的 Topic（幂等操作，已存在则跳过），无需手动去 Broker 上建 Topic。
 *
 * @author demo
 */
@Configuration
public class KafkaTopicConfig {

    /** 演示用 Topic 名称 */
    public static final String TOPIC_DEMO = "demo-topic";

    /**
     * 声明演示 Topic：3 个分区、1 个副本
     *
     * <p>说明：
     * <ul>
     *   <li>本地单机 Kafka 只能 1 副本，生产环境一般 3 副本保证高可用</li>
     *   <li>分区数建议 >= 消费者并发线程数（application.yml 中 concurrency: 2），
     *       分区是并行消费的最小单位，多余的消费者线程会空闲</li>
     * </ul>
     */
    @Bean
    public NewTopic demoTopic() {
        // NewTopic(topicName, partitionCount, replicaCount)
        return new NewTopic(TOPIC_DEMO, 3, (short) 1);
    }
}
