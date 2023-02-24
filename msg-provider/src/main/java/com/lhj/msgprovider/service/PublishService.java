package com.lhj.msgprovider.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class PublishService {

    @Autowired
    private RedisTemplate redisTemplate;

    public void sendTo(String stream, Map<String, String> msg) {
        // 创建消息记录, 以及指定stream
        StringRecord stringRecord = StreamRecords.string(msg).withStreamKey(stream);
        // 将消息添加至消息队列中
        redisTemplate.opsForStream().add(stringRecord);
    }

}
