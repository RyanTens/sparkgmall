package com.tens.dw.gmallpublisher.service;

import com.tens.dw.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImp implements PublisherService {
    @Autowired
    public DauMapper dauMapper;

    @Override
    public Long getDau(String date) {
        //从数据层读取数据，然后给Controller使用
        return dauMapper.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map> mapList = dauMapper.getHourDau(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : mapList) {

            String key = (String) map.get("LOGHOUER");
            Long value = (Long) map.get("COUNT");
            result.put(key, value);

        }

        return result;
    }
}
