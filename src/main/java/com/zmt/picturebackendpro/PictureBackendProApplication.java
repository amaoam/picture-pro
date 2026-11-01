package com.zmt.picturebackendpro;

import com.zmt.picturebackendpro.config.RabbitMqLikeProperties;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@MapperScan("com.zmt.picturebackendpro.mapper")
@EnableAspectJAutoProxy(exposeProxy = true)
@EnableAsync
@EnableConfigurationProperties(RabbitMqLikeProperties.class)
public class PictureBackendProApplication {

    public static void main(String[] args) {
        SpringApplication.run(PictureBackendProApplication.class, args);
    }

}
