package com.demo.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * SpringBoot 3.2.5 + Kafka 入门 Demo 启动类
 *
 * <p>SpringBoot3 / JDK21 注意点：
 * <ul>
 *   <li>SpringBoot3 要求 JDK17+，本项目使用 JDK21 编译运行</li>
 *   <li>SpringBoot3 全部使用 jakarta.* 包，严禁再 import javax.* 相关包</li>
 *   <li>application.yml 中开启了虚拟线程（spring.threads.virtual.enabled=true），这是 JDK21 + Boot3.2 的新特性</li>
 * </ul>
 *
 * @author demo
 */
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}
