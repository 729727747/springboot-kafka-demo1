package com.demo.kafka.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Kafka 消息生产者
 *
 * <p>【SpringBoot3 重要变更】Spring Kafka 3.x 中 KafkaTemplate.send() 的返回值
 * 已经从老的 ListenableFuture 改为 JDK 标准的 {@link CompletableFuture}
 * （SpringBoot3 移除了 ListenableFuture 体系），
 * 网上老教程中 addCallback(new ListenableFutureCallback...) 的写法在 Boot3 已不可用。
 *
 * @author demo
 */
@Component
public class KafkaMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 构造器注入（SpringBoot 官方推荐方式，便于单测，不推荐字段 @Autowired 注入）
     */
    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 异步发送（推荐方式）：不阻塞业务线程，通过 CompletableFuture 回调感知结果
     *
     * @param topic   目标 topic
     * @param key     消息 key（相同 key 进入同一分区，保证局部有序；可为 null 轮询分区）
     * @param message 消息内容
     */
    public void sendAsync(String topic, String key, String message) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);
        // JDK8+ 链式回调：成功/失败二选一执行
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                RecordMetadata meta = result.getRecordMetadata();
                log.info("[生产者-异步] 发送成功 topic={} partition={} offset={} key={}",
                        meta.topic(), meta.partition(), meta.offset(), key);
            } else {
                // 生产环境此处应做告警、重试或落库补偿，Demo 仅打印日志
                log.error("[生产者-异步] 发送失败 key={} message={}", key, message, ex);
            }
        });
    }

    /**
     * 同步发送：调用 get() 阻塞等待 Broker 确认，适合强可靠性场景（如计费、订单）
     *
     * @throws ExecutionException   Broker 返回异常
     * @throws InterruptedException 当前线程被中断
     */
    public void sendSync(String topic, String key, String message)
            throws ExecutionException, InterruptedException {
        // get() 阻塞直至收到 ack；acks=all 配置下表示所有 ISR 副本已写入
        RecordMetadata meta = kafkaTemplate.send(topic, key, message).get().getRecordMetadata();
        log.info("[生产者-同步] 发送成功 topic={} partition={} offset={}",
                meta.topic(), meta.partition(), meta.offset());
    }
}
