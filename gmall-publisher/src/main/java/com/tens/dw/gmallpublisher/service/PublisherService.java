package com.tens.dw.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //获取日活的接口
    Long getDau(String date);

    Map<String, Long> getHourDau(String date);
}
