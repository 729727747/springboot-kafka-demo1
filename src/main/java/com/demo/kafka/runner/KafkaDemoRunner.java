package com.demo.kafka.runner;

import com.demo.kafka.config.KafkaTopicConfig;
import com.demo.kafka.producer.KafkaMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * 启动自动演示：应用启动成功后自动发送 5 条消息
 *
 * <p>目的：无需手动调用 REST 接口，启动后即可在日志中看到
 * 「生产者发送 -> 消费者消费」完整链路的输出，快速验证环境是否搭建成功。
 *
 * <p>消费侧无需担心时序问题：application.yml 中 auto-offset-reset=earliest，
 * 即使消费者尚未就绪，消息发送后消费者加入消费组仍会从头拉取。
 *
 * @author demo
 */
@Component
public class KafkaDemoRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaDemoRunner.class);

    private final KafkaMessageProducer producer;

    public KafkaDemoRunner(KafkaMessageProducer producer) {
        this.producer = producer;
    }

    @Override
    public void run(String... args) {
        log.info("========== Kafka 自动演示开始：发送 5 条消息 ==========");
        for (int i = 1; i <= 5; i++) {
            producer.sendAsync(KafkaTopicConfig.TOPIC_DEMO, "demo-key-" + i, "启动自动演示消息-" + i);
        }
        log.info("========== 自动演示消息发送完毕，请查看上方 [消费者] 日志 ==========");
    }
}
