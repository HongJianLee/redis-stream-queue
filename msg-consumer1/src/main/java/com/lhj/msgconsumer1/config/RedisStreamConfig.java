package com.lhj.msgconsumer1.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "redisstream")
public class RedisStreamConfig {

    /**
     * 最多保存消息条数
     */
    private Long keepMsgNum;

    /**
     * 队列名称
     */
    private String stream;

    /**
     * 消费者组名
     */
    private String group;

    /**
     * 消费超时时间
     */
    private int consumerTimeout;

    /**
     * 消费者超时时间
     */
    private int consumerBatchSize;

}