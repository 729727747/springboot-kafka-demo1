package com.demo.kafka.consumer;

import com.demo.kafka.config.KafkaTopicConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka 消息消费者
 *
 * <p>要点说明：
 * <ul>
 *   <li>groupId 可在注解中指定，优先级高于 application.yml 中的 spring.kafka.consumer.group-id</li>
 *   <li>本 Demo 使用「手动提交 offset」（application.yml 中 enable-auto-commit=false + ack-mode=manual），
 *       业务处理完成后必须调用 {@code ack.acknowledge()}，否则 offset 不提交，
 *       应用重启后消息会被重复消费</li>
 *   <li>一个分区同一时刻只会被同一消费组内的一个线程消费</li>
 * </ul>
 *
 * @author demo
 */
@Component
public class KafkaMessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    /**
     * 监听 demo-topic，逐条消费消息
     *
     * @param record 消息记录（包含 topic/partition/offset/key/value 等元信息）
     * @param ack    手动提交句柄，仅在 ack-mode=manual 时由框架自动注入
     */
    @KafkaListener(topics = KafkaTopicConfig.TOPIC_DEMO, groupId = "demo-group")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("[消费者] 收到消息 topic={} partition={} offset={} key={} value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());

        try {
            // TODO: 这里编写真实业务逻辑（落库 / 调用下游 / 发通知等）
        } catch (Exception e) {
            // 抛出异常时框架默认会重试后跳过或进入死信，Demo 中简单打印
            log.error("[消费者] 处理消息失败 offset={}", record.offset(), e);
        } finally {
            // 业务处理成功后手动提交 offset（必须调用，否则会重复消费）
            ack.acknowledge();
        }
    }
}
