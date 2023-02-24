package com.lhj.msgprovider.controller;

import com.lhj.msgprovider.config.RedisStreamConfig;
import com.lhj.msgprovider.service.PublishService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class TestController {
    @Autowired
    private PublishService publishService;

    @Autowired
    private RedisStreamConfig redisStreamConfig;

    @GetMapping("publish/{name}")
    public Map<String, Object> test(@PathVariable("name") String name) {
        Map<String, String> map = new HashMap<>(2);
        map.put("name", name);
        publishService.sendTo(redisStreamConfig.getStream(), map);
        Map<String, Object> result = new HashMap<>(4);
        result.put("code", "200");
        result.put("msg", "success");
        result.put("data", map);
        return result;
    }
}
