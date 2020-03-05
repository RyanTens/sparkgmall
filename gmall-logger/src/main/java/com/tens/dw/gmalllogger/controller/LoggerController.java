package com.tens.dw.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tens.dw.gmall.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.lang.model.element.VariableElement;

@RestController
public class LoggerController {
    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log){
        //1.给日志添加一个时间戳
        log = addTS(log);
        //2.把日志落盘
        save2File(log);
        //3.把数据写入到kafka中
        send2Kafka(log);
        return "ok";
    }

    @Autowired  //对象自动注入
    KafkaTemplate<String,String> kafka;

    private void send2Kafka(String log) {
        String topic = Constant.STARTUP_TOPIC;
        if (log.contains("event")) {
            topic = Constant.EVENT_TOPIC;
        }
        kafka.send(topic, log);

    }

    //创建一个可以写出日志的logger对象
     Logger logger = LoggerFactory.getLogger(LoggerController.class);

    private void save2File(String log) {
        //使用log4j把数据写入到文件中
        logger.info(log);
    }

    private String addTS(String log) {
        //解析json，fastjson
        JSONObject jsonObj = JSON.parseObject(log);
        jsonObj.put("ts", System.currentTimeMillis());
        return jsonObj.toJSONString();
    }
}
