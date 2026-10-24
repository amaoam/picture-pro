package com.zmt.picturebackendpro.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.annotation.SchedulingConfigurer;

/** 点赞 Outbox 专用调度池；线程数与自适应 scheduler 的 workers 配置保持一致。 */
@Configuration
@EnableScheduling
public class CommunityLikeSchedulingConfig implements SchedulingConfigurer {
    @Value("${community.like.outbox.workers:4}")
    private int configuredWorkers;

    @Bean(name = "communityLikeTaskScheduler")
    public ThreadPoolTaskScheduler communityLikeTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(Math.max(1, Math.min(configuredWorkers, 16)));
        scheduler.setThreadNamePrefix("community-like-outbox-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(15);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setTaskScheduler(communityLikeTaskScheduler());
    }
}
