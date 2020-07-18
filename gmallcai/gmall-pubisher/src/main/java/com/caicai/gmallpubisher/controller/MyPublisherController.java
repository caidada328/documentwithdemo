package com.caicai.gmallpubisher.controller;

import com.alibaba.fastjson.JSON;
import com.caicai.gmallpubisher.bean.Option;
import com.caicai.gmallpubisher.bean.SaleInfo;
import com.caicai.gmallpubisher.bean.Stat;
import com.caicai.gmallpubisher.service.PublisherService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.RegEx;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class MyPublisherController {

    public PublisherService service;

    @GetMapping("/realtime-total")
    public String realtimeTotal(@RequestParam("date") String date) {
        ArrayList<Map<String, String>> result = new ArrayList<>();


        Map<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", service.getDau(date).toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        result.add(map2);


        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        System.out.println(service.getTotalAmount(date).toString());
        result.add(map3);


        return JSON.toJSONString(result);

    }

    @GetMapping("/realtime-hour")
    public String realtimeHOur(@RequestParam("id") String id, @RequestParam("date") String date) {

        if ("dau".equals(id)) {
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            Map<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);

            return JSON.toJSONString(result);
        }
        return "";
    }


    private String getYesterday(String today) {
        return LocalDate.parse(today).minusDays(1).toString();
    }

    @GetMapping("/sale_detail")
    public String SaleDetail(@RequestParam("date") String date,
                             @RequestParam("startpage") int startpage,
                             @RequestParam("size") int size,
                             @RequestParam("keyword") String keyword) throws Exception {

        Map<String, Object> resultGender = service.getSaleDetailAndAggGroupByField(
                date,
                keyword,
                startpage,
                size,
                "user_gender",
                2);

        Map<String, Object> resultAge = service.getSaleDetailAndAggGroupByField(
                date,
                keyword,
                startpage,
                size,
                "user_age",
                100);

        SaleInfo saleInfo = new SaleInfo();

        saleInfo.setTotal((Integer) resultAge.get("agg"));
        List<Map<String, Object>> detail = (List<Map<String, Object>>) resultAge;
        saleInfo.setDetail(detail);

        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        Map<String, Long> genderAgg = (Map<String, Long>) resultGender.get("agg");

        for (String key : genderAgg.keySet()) {
            Option opt = new Option();
            opt.setName(key.replace("F", "女").replace("M", "男"));
            opt.setValue(genderAgg.get(key));
        }
        saleInfo.addStat(genderStat);


        Stat ageStat = new Stat();
        ageStat.addOption(new Option("20岁以下", 0L));
        ageStat.addOption(new Option("20岁到30岁", 0L));
        ageStat.addOption(new Option("30岁及以上", 0L));
        ageStat.setTitle("用户年龄占比");
        Map<String, Long> ageAgg = (Map<String, Long>) resultAge.get("agg");
        for (String key : ageAgg.keySet()) {
            int age = Integer.parseInt(key);
            Long value = ageAgg.get(key);
            if (age < 20) {
                Option opt = ageStat.getOptions().get(0);
                opt.setValue((opt.getValue() + value));
            }
            if (age < 30) {
                Option opt = ageStat.getOptions().get(1);
                opt.setValue((opt.getValue() + value));
            }
            if (age < 20) {
                Option opt = ageStat.getOptions().get(2);
                opt.setValue((opt.getValue() + value));
            }

        }
        saleInfo.addStat(ageStat);
        return JSON.toJSONString(saleInfo);
    }
}