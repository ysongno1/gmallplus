package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //分流出来hbase侧输出流的格式:
    // {"sink_table":"dim_base_trademark","database":"gmallplus","after":{"tm_name":"香奈儿","id":11},"type":"insert","tableName":"base_trademark"}
    //TODO 将数据写入phoniex: upsert into table(id,tm_name)values(...,...)
    @Override
    public void invoke(JSONObject value, Context context) {

        PreparedStatement preparedStatement = null;

        try {
            //获取数据中的key以及value
            JSONObject after = value.getJSONObject("after");
            Set<String> columns = after.keySet();
            Collection<Object> values = after.values();

            //获取表名
            String sinkTable = value.getString("sink_table");

            //获取插入sql语句
            String upsertSQL = getUpsertSQL(columns, values, sinkTable);

            //打印sql语句
            System.out.println(upsertSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSQL);

            //TODO 判断如果当前数据为更新操作，则先删除Redis中的缓存数据
            if ("update".equals(value.get("type"))){
                DimUtil.deleteDimInfo(sinkTable.toUpperCase(Locale.ROOT), after.getString("id"));
            }

            //执行sql
            preparedStatement.execute();

            connection.commit();

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("表"+value.getString("sink_table")+"插入数据失败");
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private String getUpsertSQL(Set<String> columns, Collection<Object> values, String sinkTable) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }


}

