package com.demo.kafka.controller;

import java.util.concurrent.ExecutionException;

import com.demo.kafka.config.KafkaTopicConfig;
import com.demo.kafka.producer.KafkaMessageProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 演示用 REST 接口：手动发消息验证「生产 -> 消费」链路
 *
 * <p>接口列表：
 * <ul>
 *   <li>GET /api/kafka/send       —— 异步发送</li>
 *   <li>GET /api/kafka/send-sync  —— 同步发送（等 Broker 确认）</li>
 * </ul>
 *
 * @author demo
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaDemoController {

    private final KafkaMessageProducer producer;

    public KafkaDemoController(KafkaMessageProducer producer) {
        this.producer = producer;
    }

    /**
     * 异步发送消息
     * 示例：GET http://localhost:8080/api/kafka/send?key=k1&message=hello
     */
    @GetMapping("/send")
    public String send(@RequestParam(required = false) String key,
                       @RequestParam(defaultValue = "hello-kafka") String message) {
        producer.sendAsync(KafkaTopicConfig.TOPIC_DEMO, key, message);
        return "异步消息已提交 -> topic=" + KafkaTopicConfig.TOPIC_DEMO + ", key=" + key + ", message=" + message;
    }

    /**
     * 同步发送消息（阻塞等待 Broker 确认后才返回）
     * 示例：GET http://localhost:8080/api/kafka/send-sync?message=hello
     */
    @GetMapping("/send-sync")
    public String sendSync(@RequestParam(defaultValue = "hello-kafka-sync") String message)
            throws ExecutionException, InterruptedException {
        producer.sendSync(KafkaTopicConfig.TOPIC_DEMO, "sync-key", message);
        return "同步消息已被 Broker 确认 -> " + message;
    }
}
