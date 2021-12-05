package com.atguigu.gmallpublisher1.service;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Desc: 商品统计接口
 */
public interface ProductStatsService {
    //获取某一天的总交易额
    BigDecimal getGMV(int date);

    //从Mapper查询出来的数据格式
    //List[Map(tm_name-> TCL, order_amount->123456),
    //     Map(tm_name-> 苹果, order_amount->123456)
    //     ......]
    // ====>转换格式
    //Map[(TCL->123456),(苹果->123456)]
    Map selectGmvByTm (int date, int limit);
}

