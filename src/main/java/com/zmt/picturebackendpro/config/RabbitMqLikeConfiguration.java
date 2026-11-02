package com.zmt.picturebackendpro.config;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 拓扑与监听器配置。
 *
 * <p>仅在配置项 {@code community.like.mode=rabbitmq} 时生效，用于支撑社区点赞的异步处理链路。</p>
 *
 * <p>整体拓扑结构：</p>
 * <pre>
 *  生产者
 *    └─> 主交换机 communityLikeExchange
 *          └─> 主队列 communityLikeQueue
 *                ├─ 消费成功：手动 ACK
 *                ├─ 消费失败且不重新入队：进入死信交换机 communityLikeDeadExchange
 *                │     └─> 死信队列 communityLikeDeadQueue（用于审计/告警）
 *                └─ 需要延迟重试：投递到重试交换机 communityLikeRetryExchange
 *                      └─> 重试队列 communityLikeRetryQueue（设置消息 TTL）
 *                            └─ TTL 到期后通过 DLX 回到主交换机 communityLikeExchange
 * </pre>
 */
@Configuration
@EnableRabbit
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
@EnableConfigurationProperties(RabbitMqLikeProperties.class)
public class RabbitMqLikeConfiguration {

    /**
     * RabbitAdmin 用于在应用启动时自动声明（declare）Spring 容器中的
     * Exchange、Queue、Binding，保证 RabbitMQ 服务端拓扑与代码配置一致。
     */
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
        // 启动时自动加载并声明拓扑
        rabbitAdmin.setAutoStartup(true);
        return rabbitAdmin;
    }

    /**
     * 主交换机：用于接收点赞事件消息，类型为 Direct（按 routingKey 精确路由）。
     *
     * @param p RabbitMQ 点赞相关配置属性
     * @return 持久化的主交换机
     */
    @Bean
    public DirectExchange communityLikeExchange(RabbitMqLikeProperties p) {
        // 参数依次为：交换机名称、是否持久化、是否自动删除
        return new DirectExchange(p.getExchange(), true, false);
    }

    /**
     * 主队列：接收点赞事件消息，由批量监听器消费。
     *
     * <p>配置了死信交换机与死信路由键，当消息被拒绝（不重新入队）或达到投递上限时，
     * 会被路由到死信队列，便于审计与问题排查。</p>
     *
     * @param p RabbitMQ 点赞相关配置属性
     * @return 持久化主队列
     */
    @Bean
    public Queue communityLikeQueue(RabbitMqLikeProperties p) {
        return QueueBuilder.durable(p.getQueue())
                // 死信交换机：消息成为死信后投递到该交换机
                .withArgument("x-dead-letter-exchange", p.getDeadExchange())
                // 死信路由键：死信投递时使用的 routingKey
                .withArgument("x-dead-letter-routing-key", p.getDeadRoutingKey())
                .build();
    }

    /**
     * 主队列与主交换机的绑定关系，使用配置中的主 routingKey 进行匹配。
     */
    @Bean
    public Binding communityLikeBinding(@Qualifier("communityLikeQueue") Queue queue,
                                        @Qualifier("communityLikeExchange") DirectExchange exchange,
                                        RabbitMqLikeProperties p) {
        return BindingBuilder.bind(queue).to(exchange).with(p.getRoutingKey());
    }

    /**
     * 死信交换机：接收从主队列转发过来的死信消息。
     */
    @Bean
    public DirectExchange communityLikeDeadExchange(RabbitMqLikeProperties p) {
        return new DirectExchange(p.getDeadExchange(), true, false);
    }

    /**
     * 死信队列：存放主队列产生的死信消息，通常由独立的死信审计消费者处理。
     */
    @Bean
    public Queue communityLikeDeadQueue(RabbitMqLikeProperties p) {
        return QueueBuilder.durable(p.getDeadQueue()).build();
    }

    /**
     * 死信队列与死信交换机的绑定关系，使用死信 routingKey 进行匹配。
     */
    @Bean
    public Binding communityLikeDeadBinding(@Qualifier("communityLikeDeadQueue") Queue queue,
                                            @Qualifier("communityLikeDeadExchange") DirectExchange exchange,
                                            RabbitMqLikeProperties p) {
        return BindingBuilder.bind(queue).to(exchange).with(p.getDeadRoutingKey());
    }

    /**
     * 重试交换机：用于承接需要延迟重试的消息。
     *
     * <p>重试队列中的消息 TTL 到期后，会通过队列自身的 DLX（死信交换机）重新回到主交换机，
     * 从而实现“延迟重试”的效果。</p>
     */
    @Bean
    public DirectExchange communityLikeRetryExchange(RabbitMqLikeProperties p) {
        return new DirectExchange(p.getRetryExchange(), true, false);
    }

    /**
     * 重试队列：消息在该队列中等待 TTL 到期，到期后通过 DLX 自动回到主交换机。
     *
     * <p>注意：队列本身不设置统一的 x-message-ttl，而是由生产者在发送消息时通过
     * expiration 属性为每条消息单独设置延迟时间，以便支持不同退避时长。</p>
     *
     * @param p RabbitMQ 点赞相关配置属性
     * @return 持久化重试队列
     */
    @Bean
    public Queue communityLikeRetryQueue(RabbitMqLikeProperties p) {
        return QueueBuilder.durable(p.getRetryQueue())
                // TTL 到期后，消息作为死信投递回主交换机
                .withArgument("x-dead-letter-exchange", p.getExchange())
                // 回到主交换机时使用的 routingKey 与主队列一致
                .withArgument("x-dead-letter-routing-key", p.getRoutingKey())
                .build();
    }

    /**
     * 重试队列与重试交换机的绑定关系，使用重试 routingKey 进行匹配。
     */
    @Bean
    public Binding communityLikeRetryBinding(@Qualifier("communityLikeRetryQueue") Queue queue,
                                             @Qualifier("communityLikeRetryExchange") DirectExchange exchange,
                                             RabbitMqLikeProperties p) {
        return BindingBuilder.bind(queue).to(exchange).with(p.getRetryRoutingKey());
    }

    /**
     * 主点赞消费者使用的批量监听容器工厂。
     *
     * <p>关键特性：</p>
     * <ul>
     *     <li>手动 ACK：由业务代码在成功处理后显式确认，便于批量提交；</li>
     *     <li>批量监听：一次接收多条消息，减少数据库/RPC 往返；</li>
     *     <li>单消费者顺序消费：保证同一队列内消息处理顺序稳定；</li>
     *     <li>拒绝消息不重新入队：失败消息交由死信/重试链路处理，避免无限 requeue。</li>
     * </ul>
     */
    @Bean(name = "communityLikeBatchListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory communityLikeBatchListenerContainerFactory(
            ConnectionFactory connectionFactory, RabbitMqLikeProperties p) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        // 手动 ACK：由监听器方法调用 channel.basicAck 等方式确认
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        // 开启批量监听模式
        factory.setBatchListener(true);
        // 开启消费者端批量接收（配合 batchSize 使用）
        factory.setConsumerBatchEnabled(true);
        // 每批最大消息数，至少为 1
        factory.setBatchSize(Math.max(1, p.getConsumerBatchSize()));
        // 接收超时时间（毫秒），至少为 1
        factory.setReceiveTimeout(Math.max(1L, p.getConsumerReceiveTimeoutMs()));
        // 预取数量：每个消费者最多预取多少条未确认消息，至少为 1
        factory.setPrefetchCount(Math.max(1, p.getConsumerPrefetch()));
        // 保留当前已验证的单消费者顺序消费策略；并发可在独立实验中调整。
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        // 拒绝消息时不重新入队，避免失败消息反复回环，交由死信/重试链路处理
        factory.setDefaultRequeueRejected(false);
        return factory;
    }

    /**
     * 死信审计消费者使用的监听容器工厂。
     *
     * <p>死信审计消费者独立运行，不占用主点赞消费者线程，避免审计逻辑拖慢主链路。</p>
     *
     * <p>关键特性：</p>
     * <ul>
     *     <li>手动 ACK：审计成功后再确认；</li>
     *     <li>单消费者、prefetch=1：保证逐条处理与顺序；</li>
     *     <li>defaultRequeueRejected=true：审计失败时允许重新入队，避免死信消息丢失。</li>
     * </ul>
     */
    @Bean(name = "communityLikeDeadLetterListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory communityLikeDeadLetterListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        // 手动 ACK，审计处理完成后显式确认
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        // 单消费者，保证死信消息逐条有序处理
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        // 每次只预取 1 条，降低失败时的影响范围
        factory.setPrefetchCount(1);
        // 审计失败时允许重新入队，避免死信消息被丢弃
        factory.setDefaultRequeueRejected(true);
        return factory;
    }
}