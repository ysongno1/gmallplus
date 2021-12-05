package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> hbaseTag;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> hbaseTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.hbaseTag = hbaseTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化 注册驱动 获取连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        /*
            因为主流和广播流是平级的 主流到了广播流可能还未到达  主流方法里判断tableProcess 就为空  走else报key不存在
            解决办法：我们可以在open方法里把mySQL配置表读到内存的map的集合中 然后在打印没有key之前去内存尝试获取配置表信息
         */
    }

    //广播流： cdc后数据的格式：{"db":"", "after":{"":"","":""},"type":"","tableName":""}
    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        //1. 将after中的数据解析成JavaBean
        JSONObject jsonObject = JSON.parseObject(s);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.检验HBase表是否存在 不存在则建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.数据写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tableProcess.getSourceTable() + "-" + tableProcess.getOperateType(), tableProcess);

    }

    //在HBase上创建表
    //create table if not exists db.tn(id varchar primary key, name varchar) sinkExtend
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }

            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                //取出列名
                String column = columns[i];
                createTableSql.append(column).append(" varchar");

                //判断是否为主键
                if (column.equals(sinkPk)) {
                    createTableSql.append(" primary key");
                }

                //判断是不是最后一个字段 则添加","
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }

            createTableSql.append(")").append(sinkExtend);

            //打印建表语句
            System.out.println(createTableSql);

            //执行建表操作
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表:" + sinkTable + "失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //主流： base_trademark的数据格式
    // {"database":"gmallplus","after":{"tm_name":"香奈儿","logo_url":"/static/default.jpg","id":11},"type":"insert","tableName":"base_trademark"}
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //1.根据主键读取广播状态对应的数据
        String key = jsonObject.getString("tableName") + "-" + jsonObject.getString("type");
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);

        //因为在配置表我们只需要34张表 而我们的业务数据库里有46张表，也就是主流里有46张表的数据  所以主流在配置表的广播流get到的key可能为空
        if (tableProcess != null) {

            //2.根据广播状态数据  过滤字段
            filterCloumns(jsonObject.getJSONObject("after"), tableProcess.getSinkColumns());

            //3.根据广播状态数据  分流   主流：Kafka   侧输出流：HBase
            jsonObject.put("sink_table", tableProcess.getSinkTable()); //原因：主流数据并没有要写入的dwd/dim的主题或表名 无法进行下一步

            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                readOnlyContext.output(hbaseTag, jsonObject);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                collector.collect(jsonObject);
            }

        } else {

            //从内存的Map中尝试获取数据
            System.out.println(key + "不存在！");
        }

    }

    private void filterCloumns(JSONObject after, String sinkColumns) {

        String[] split = sinkColumns.split(",");
        List<String> sinkColumnList = Arrays.asList(split);

/*
        Set<Map.Entry<String, Object>> entries = after.entrySet();
//        System.out.println(entries);  //[tm_name=Redmi, id=1]
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
//           System.out.println(next);//tm_name=Redmi
//                                     // id=1
            if (!sinkColumnList.contains(next.getKey())) {
                iterator.remove();
            }
        }
*/

        Set<Map.Entry<String, Object>> entries = after.entrySet();
        entries.removeIf(next -> !sinkColumnList.contains(next.getKey()));

    }


}
