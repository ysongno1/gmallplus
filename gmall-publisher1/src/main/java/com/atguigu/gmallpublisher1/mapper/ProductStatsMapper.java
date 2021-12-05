package com.atguigu.gmallpublisher1.mapper;


import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Desc: 商品统计Mapper
 */
public interface ProductStatsMapper {

    //获取商品交易额
    @Select("select sum(order_amount) from product_stats where toYYYYMMDD(stt)=${date}")
    BigDecimal getGMV(int date);

    //获取各个品牌的交易额 可以用Map也可以用JavaBean 而且JavaBean的字段可以比查的字段多
    @Select("select tm_name,sum(order_amount) order_amount from product_stats_210625 where toYYYYMMDD(stt)=${date} group by tm_name order by order_amount desc limit ${limit}")
    List<Map> selectGmvByTm(@Param("date") int date, @Param("limit") int limit);
 }
