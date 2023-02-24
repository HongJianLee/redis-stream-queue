package com.lhj.msgconsumer1.listener;

import cn.hutool.json.JSONUtil;
import com.lhj.common.constant.Constant;
import com.lhj.msgconsumer1.config.RedisStreamConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class StreamMessageListener implements StreamListener<String, MapRecord<String, String, String>> {

    @Autowired
    private RedisStreamConfig redisStreamConfig;

    @Autowired
    private RedisTemplate redisTemplate;


    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        try {
            log.info("StreamMessageListener  stream message。messageId={}, stream={}, body={}", message.getId(),
                    message.getStream(), JSONUtil.toJsonStr(message.getValue()));
            if (Constant.MSG.equals(message.getValue().get(Constant.MSG))) {
                return;
            }
            Thread.sleep(3000);
            log.info("任务处理完成！");
        } catch (Exception e) {
            log.error("任务处理失败：", e);
        } finally {
            // 通过RedisTemplate手动确认消息
            redisTemplate.opsForStream().acknowledge(redisStreamConfig.getGroup(), message);
        }
    }
}
