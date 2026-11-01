package com.zmt.picturebackendpro.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** RabbitMQ 拓扑与批量监听器配置。仅 community.like.mode=rabbitmq 时生效。 */
@Configuration
@EnableRabbit
@ConditionalOnProperty(name = "community.like.mode", havingValue = "rabbitmq")
@EnableConfigurationProperties(RabbitMqLikeProperties.class)
public class RabbitMqLikeConfiguration {

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
        // 设置启动时自动加载 Spring 容器中的 Exchange、Queue、Binding
        rabbitAdmin.setAutoStartup(true);
        return rabbitAdmin;
    }

    @Bean
    public DirectExchange communityLikeExchange(RabbitMqLikeProperties p) {
        return new DirectExchange(p.getExchange(), true, false);
    }

    @Bean
    public Queue communityLikeQueue(RabbitMqLikeProperties p) {
        // 单消费者 + requeue 保留顺序；出错消息达到 delivery limit 前由应用用 eventId 做持久化重试计数。
        return QueueBuilder.durable(p.getQueue())
                .withArgument("x-dead-letter-exchange", p.getDeadExchange())
                .withArgument("x-dead-letter-routing-key", p.getDeadRoutingKey())
                .build();
    }

    @Bean
    public Binding communityLikeBinding(@Qualifier("communityLikeQueue") Queue communityLikeQueue,
                                        @Qualifier("communityLikeExchange") DirectExchange communityLikeExchange,
                                        RabbitMqLikeProperties p) {
        return BindingBuilder.bind(communityLikeQueue).to(communityLikeExchange).with(p.getRoutingKey());
    }

    @Bean
    public DirectExchange communityLikeDeadExchange(RabbitMqLikeProperties p) {
        return new DirectExchange(p.getDeadExchange(), true, false);
    }

    @Bean
    public Queue communityLikeDeadQueue(RabbitMqLikeProperties p) {
        return QueueBuilder.durable(p.getDeadQueue()).build();
    }

    @Bean
    public Binding communityLikeDeadBinding(@Qualifier("communityLikeDeadQueue") Queue communityLikeDeadQueue,
                                            @Qualifier("communityLikeDeadExchange") DirectExchange communityLikeDeadExchange,
                                            RabbitMqLikeProperties p) {
        return BindingBuilder.bind(communityLikeDeadQueue).to(communityLikeDeadExchange)
                .with(p.getDeadRoutingKey());
    }

    /** 消息在该队列中等待 message expiration，过期后由 DLX 自动回到主队列。 */
    @Bean
    public Queue communityLikeRetryQueue(RabbitMqLikeProperties p) {
        return QueueBuilder.durable(p.getRetryQueue())
                .withArgument("x-dead-letter-exchange", p.getExchange())
                .withArgument("x-dead-letter-routing-key", p.getRoutingKey())
                .build();
    }

    @Bean
    public Binding communityLikeRetryBinding(@Qualifier("communityLikeRetryQueue") Queue queue,
                                             @Qualifier("communityLikeRetryExchange") DirectExchange exchange,
                                             RabbitMqLikeProperties p) {
        return BindingBuilder.bind(queue).to(exchange).with(p.getRetryRoutingKey());
    }

    @Bean(name = "communityLikeBatchListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory communityLikeBatchListenerContainerFactory(
            ConnectionFactory connectionFactory, RabbitMqLikeProperties p) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setBatchListener(true);
        factory.setConsumerBatchEnabled(true);
        factory.setBatchSize(Math.max(1, p.getConsumerBatchSize()));
        factory.setReceiveTimeout(Math.max(1L, p.getConsumerReceiveTimeoutMs()));
        factory.setPrefetchCount(Math.max(1, p.getConsumerPrefetch()));
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(1);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}
