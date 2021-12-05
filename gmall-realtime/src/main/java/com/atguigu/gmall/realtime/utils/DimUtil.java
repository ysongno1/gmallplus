package com.atguigu.gmall.realtime.utils;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/*
    Handle 对JDBCUtil的SQL语句进行封装
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String table, String key) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {

        //TODO 查询Phoenix之前先查询redis缓存
        Jedis jedis = RedisUtil.getJedis();
        //设计redis中的key DIM:DIM_USER_INFO:id
        String rediskey = "DIM:" + table + ":" + key;
        String dimInfo = jedis.get(rediskey);
        //TODO 在reidis中查询不到往下走去phoenix 查到了就返回
        if (dimInfo != null) {

            //重置过期时间
            jedis.expire(rediskey, 24 * 60 * 60);

            //归还连接
            jedis.close();


            //返回结果
            return JSONObject.parseObject(dimInfo);
        }


        //TODO 构建查询语句
        String querysql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + table + " where id = '" + key + "'";

        //TODO 执行查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querysql, JSONObject.class, false);//未转化为JavaBean所以false

        //TODO 在返回phoenix查询结果之前 将数据写入Redis中 方便下一次查询
        JSONObject dimInfoJson = queryList.get(0);
        jedis.set(rediskey, dimInfoJson.toJSONString());
        jedis.expire(rediskey, 24 * 60 * 60);
        jedis.close();

        return dimInfoJson; //限制了where id 所以只用一条数据

    }

    //TODO 删除Redis中的数据
    public static void deleteDimInfo(String table, String key){

        Jedis jedis = RedisUtil.getJedis();

        String redisKey = "DIM:" + table + ":" + key;

        jedis.del(redisKey);

        jedis.close();

    }

}
