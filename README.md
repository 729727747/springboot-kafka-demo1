# springboot-kafka-demo

SpringBoot **3.2.5** + **JDK21** + **Kafka** 入门学习 Demo，演示消息的同步/异步发送与手动提交消费。

## 一、环境要求

| 软件 | 版本 | 说明 |
|---|---|---|
| JDK | 21 | SpringBoot3 最低要求 17，本项目按 21 编译 |
| Maven | 3.6.3+ | 只用 Maven，不用 Gradle |
| Kafka | 3.x | 本地单机即可，无需 ZooKeeper（KRaft 模式） |

## 二、Kafka 本地部署（Docker 一键启动，推荐）

```bash
# KRaft 模式单容器，默认配置即适配 localhost:9092
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0

# 验证容器状态
docker ps | findstr kafka        # Windows PowerShell
docker logs -f kafka             # 看到 "Kafka Server started" 即成功
```

> 没有 Docker 也可从 https://kafka.apache.org/downloads 下载安装包，KRaft 模式启动（README 不展开）。

## 三、构建与启动

```bash
# 1. 拉取依赖并编译（首次执行会下载依赖）
mvn clean install -DskipTests

# 2. 启动应用（需先确保 Kafka 已在 localhost:9092 运行）
mvn spring-boot:run

# 或打包后直接运行 fat jar
mvn clean package -DskipTests
java -jar target/springboot-kafka-demo-1.0.0.jar
```

## 四、测试方法

### 1. 启动自动演示（无需任何操作）

应用启动后 `KafkaDemoRunner` 会自动发送 5 条消息，控制台直接可见完整链路日志：

```
[生产者-异步] 发送成功 topic=demo-topic partition=2 offset=0 key=demo-key-1
[消费者] 收到消息 topic=demo-topic partition=2 offset=0 key=demo-key-1 value=启动自动演示消息-1
```

### 2. REST 接口手动测试

```bash
# 异步发送
curl "http://localhost:8080/api/kafka/send?key=k1&message=hello-kafka"

# 同步发送（等 Broker 确认后才返回）
curl "http://localhost:8080/api/kafka/send-sync?message=hello-sync"
```

浏览器访问同样生效：http://localhost:8080/api/kafka/send?message=hello

## 五、核心演示点

| 演示点 | 对应文件 |
|---|---|
| Topic 自动创建（NewTopic Bean） | `config/KafkaTopicConfig.java` |
| 异步发送 + CompletableFuture 回调 | `producer/KafkaMessageProducer.java#sendAsync` |
| 同步发送（get() 阻塞确认） | `producer/KafkaMessageProducer.java#sendSync` |
| @KafkaListener 消费 + 手动提交 offset | `consumer/KafkaMessageConsumer.java` |
| 启动自动验证链路 | `runner/KafkaDemoRunner.java` |

## 六、SpringBoot3 / JDK21 常见踩坑提示

1. **jakarta 包问题**：SpringBoot3 全部使用 `jakarta.*` 包（如 `jakarta.annotation.Resource`），
   直接复制老项目代码出现 `import javax.*` 会编译失败，需全局替换。

2. **KafkaTemplate 返回值变更**：Spring Kafka 3.x（Boot3 配套）中 `send()` 返回
   `CompletableFuture<SendResult>`，老教程的 `ListenableFuture` + `addCallback` 写法已废弃。

3. **JDK21 虚拟线程**：`spring.threads.virtual.enabled=true` 仅 Boot 3.2+ 支持，
   IO 密集型接口吞吐提升明显；但线程池相关自定义代码不要假设平台线程，谨慎混用 synchronized 长阻塞代码。

4. **依赖下载失败**：默认走 Maven 中央仓库。国内网络拉取慢/失败，可在 `~/.m2/settings.xml` 配置阿里云镜像：

   ```xml
   <mirror>
       <id>aliyun</id>
       <mirrorOf>central</mirrorOf>
       <url>https://maven.aliyun.com/repository/public</url>
   </mirror>
   ```

5. **连接不上 Kafka**：确认 `bootstrap-servers: localhost:9092` 与实际部署一致；
   Docker 跨机访问需注意 advertised.listeners 配置（本机学习无此问题）。

6. **消息重复消费**：本 Demo 使用手动提交 offset，业务处理完必须 `ack.acknowledge()`，
   忘记提交则重启后从上次提交位置重新消费。

## 七、项目结构

```
springboot-kafka-demo
├── pom.xml
├── README.md
└── src/main
    ├── java/com/demo/kafka
    │   ├── KafkaDemoApplication.java        # 启动类
    │   ├── config/KafkaTopicConfig.java     # Topic 自动创建
    │   ├── producer/KafkaMessageProducer.java # 同步/异步发送
    │   ├── consumer/KafkaMessageConsumer.java # 消费 + 手动提交
    │   ├── controller/KafkaDemoController.java # REST 测试接口
    │   └── runner/KafkaDemoRunner.java      # 启动自动演示
    └── resources/application.yml            # 最小必要配置
```
