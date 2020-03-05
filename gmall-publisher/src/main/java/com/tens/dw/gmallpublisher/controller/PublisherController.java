package com.tens.dw.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.tens.dw.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    public PublisherService service;


    @GetMapping("/realtime-total")
    public String getDau(@RequestParam("date") String date){
        List<Map<String, String>> result = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", service.getDau(date) + "");
        result.add(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map1.put("id", "new_mid");
        map1.put("name", "新增设备");
        map1.put("value", "233");
        result.add(map2);

        return JSON.toJSONString(result);
    }

    @GetMapping("realtime-hour")
    public String getHourCount(@RequestParam("id") String id, @RequestParam("date") String date){
        if("dau".equals(id)){
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            HashMap<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);

        }else if("".equals(id)){

        }

        return null;
    }

    private String getYesterday(String date){
        return LocalDate.parse(date).minusDays(1).toString();
    }

}
