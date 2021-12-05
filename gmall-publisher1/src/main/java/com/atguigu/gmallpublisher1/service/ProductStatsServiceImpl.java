package com.atguigu.gmallpublisher1.service;

import com.atguigu.gmallpublisher1.mapper.ProductStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProductStatsServiceImpl implements ProductStatsService{

    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

    @Override
    public Map selectGmvByTm(int date, int limit) {

        //查询ClickHouse获取品牌GMV数量
        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);

        //创建Map用于存放最终结果数据
        HashMap<String, BigDecimal> resultMap = new HashMap<>();

        //遍历mapList,将数据取出放入resultMap
        for (Map map : mapList) {
            resultMap.put((String) map.get("tm_name"), (BigDecimal) map.get("order_amount"));
        }

        //返回结果集
        return resultMap;
    }
}
