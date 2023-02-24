package com.lhj.msgprovider.scheduled;

import com.lhj.msgprovider.config.RedisStreamConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class CleanStreamJob {

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private RedisStreamConfig redisStreamConfig;


    @Scheduled(cron = "0/5 * * * * ?")
    public void cleanStreamJob() {
        log.info("cleanStreamJob----->");
        if (redisTemplate.hasKey(redisStreamConfig.getStream())) {
            // 定时的清理stream中的数据，保留适当条数
            redisTemplate.opsForStream().trim(redisStreamConfig.getStream(), redisStreamConfig.getKeepMsgNum());
        }
    }
}
