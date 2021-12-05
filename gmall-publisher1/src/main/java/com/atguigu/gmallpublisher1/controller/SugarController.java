package com.atguigu.gmallpublisher1.controller;

import com.atguigu.gmallpublisher1.service.ProductStatsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("api/sugar")
public class SugarController {

    @Autowired
    ProductStatsService productStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }

        BigDecimal gmv = productStatsService.getGMV(date);

        return "{ " +
                "              \"status\": 0, " +
                "              \"msg\": \"\", " +
                "              \"data\":" + gmv +
                "            }";
    }


    @RequestMapping("/trademark")
    public String getGMVByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit") int limit) {
        if (date == 0) {
            date = getToday();
        }

        //查询数据
        Map gmvByTm = productStatsService.selectGmvByTm(date, limit);

        //取出Map的Key与value
        Set keySet = gmvByTm.keySet();
        Collection values = gmvByTm.values();

        //拼接并返回结果
        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [\"" +
                StringUtils.join(keySet, "\",\"") +
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"商品品牌\", " +
                "        \"data\": [" +
                StringUtils.join(values, ",") +
                "] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";

    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();
        return Integer.parseInt(sdf.format(ts));
    }

}
