package com.lhj.msgconsumer1.config;

import cn.hutool.core.util.IdUtil;
import com.lhj.common.constant.Constant;
import com.lhj.msgconsumer1.listener.StreamMessageListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class RedisStreamConsumerConfig {

    @Resource
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private RedisStreamConfig redisStreamConfig;

    @Autowired
    private RedisTemplate redisTemplate;

    /**
     * 第一次设置在线消费者超时时间
     */
    private final Long FIRST_TIMEOUT;

    /**
     * 在线消费者超时时间续期
     */
    private final Long REDIS_TIMEOUT;

    /**
     * 当前服务消费者
     */
    private final String CUR_CONSUMER;

    public RedisStreamConsumerConfig() {
        this.FIRST_TIMEOUT = 50L;
        this.REDIS_TIMEOUT = 40L;
        this.CUR_CONSUMER = "consumer-".concat(IdUtil.fastSimpleUUID());
    }

    @Scheduled(cron = "0/30 * * * * ?")
    public void expireConsumer() {
        String queueGroupConsumer = Constant.MSG_QUEUE.concat(redisStreamConfig.getGroup()).concat(":").concat(Constant.MSG_CONSUMER);
        String curConsumerKey = queueGroupConsumer.concat(CUR_CONSUMER);
        // 给当前消费者续期，剔除已下线的consumer
        if (redisTemplate.hasKey(curConsumerKey)) {
            redisTemplate.expire(curConsumerKey, REDIS_TIMEOUT, TimeUnit.SECONDS);
        }
        // 在线消费者列表
        Set<String> keys = redisTemplate.keys(queueGroupConsumer.concat("*"));
        List<String> consumerNameList = redisTemplate.opsForValue().multiGet(keys);
        // 队列组中消费者列表
        StreamInfo.XInfoConsumers xInfoConsumers = redisTemplate.opsForStream().consumers(redisStreamConfig.getStream(),
                redisStreamConfig.getGroup());
        List<String> consumerList = xInfoConsumers.stream()
                .map(item -> item.consumerName())
                .collect(Collectors.toList());
        consumerList.removeAll(consumerNameList);

        for (String name : consumerList) {
            log.info("删除下线消费者：{}", name);
            redisTemplate.opsForStream().deleteConsumer(redisStreamConfig.getStream(), Consumer.from(redisStreamConfig.getGroup(), name));
        }
    }

    /**
     * 主要做的是将MailStreamListener监听绑定消费者，用于接收消息
     *
     * @param connectionFactory connectionFactory
     * @param streamListener    streamListener
     * @return StreamMessageListenerContainer
     */
    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> consumerListener(
            RedisConnectionFactory connectionFactory, StreamMessageListener streamListener) {
        this.initQueueAndGroup();
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = streamContainer(
                redisStreamConfig.getStream(), connectionFactory, streamListener);
        container.start();
        return container;
    }

    /**
     * 初始化队列和组
     */
    private void initQueueAndGroup() {
        if (!redisTemplate.hasKey(redisStreamConfig.getStream())) {
            // 创建消息记录, 以及指定stream
            Map<String, String> msg = new HashMap<>();
            msg.put(Constant.MSG, Constant.MSG);
            StringRecord stringRecord = StreamRecords.string(msg).withStreamKey(redisStreamConfig.getStream());
            // 将消息添加至消息队列中
            redisTemplate.opsForStream().add(stringRecord);
        }
        StreamInfo.XInfoStream xInfoStream = redisTemplate.opsForStream().info(redisStreamConfig.getStream());
        if (xInfoStream.groupCount() < 1) {
            redisTemplate.opsForStream().createGroup(redisStreamConfig.getStream(), redisStreamConfig.getGroup());
        }
    }

    /**
     * @param mystream          从哪个流接收数据
     * @param connectionFactory connectionFactory
     * @param streamListener    绑定的监听类
     * @return StreamMessageListenerContainer
     */
    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamContainer(String mystream,
                                                                                                      RedisConnectionFactory connectionFactory,
                                                                                                      StreamListener<String, MapRecord<String, String, String>> streamListener) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                .pollTimeout(Duration.ofSeconds(redisStreamConfig.getConsumerTimeout())) // 拉取消息超时时间
                .batchSize(redisStreamConfig.getConsumerBatchSize()) // 批量抓取消息
                .executor(taskExecutor).build();
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer.create(
                connectionFactory, options);
        //指定消费最新的消息
        StreamOffset<String> offset = StreamOffset.create(mystream, ReadOffset.lastConsumed());
        //创建消费者
        Consumer consumer = Consumer.from(redisStreamConfig.getGroup(), CUR_CONSUMER);
        StreamMessageListenerContainer.StreamReadRequest<String> streamReadRequest = StreamMessageListenerContainer.StreamReadRequest.builder(
                        offset).errorHandler((error) -> {
                }).cancelOnError(e -> false).consumer(consumer)
                //关闭自动ack确认
                .autoAcknowledge(false).build();
        //指定消费者对象
        container.register(streamReadRequest, streamListener);

        String queueGroupConsumer = Constant.MSG_QUEUE.concat(redisStreamConfig.getGroup()).concat(":").concat(Constant.MSG_CONSUMER);
        String curConsumerKey = queueGroupConsumer.concat(CUR_CONSUMER);
        redisTemplate.opsForValue().set(curConsumerKey, CUR_CONSUMER, FIRST_TIMEOUT, TimeUnit.SECONDS);
        return container;
    }

}
