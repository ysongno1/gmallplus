package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {

        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建Flink-MySQL-CDC的Source
        tableEnv
                .executeSql("create table user_info(" +
                        "id int," +
                        "name string," +
                        "phone_num string" +
                        ")with(" +
                        "'connector' = 'mysql-cdc'," +
                        "'hostname' = 'hadoop102'," +
                        "'port' = '3306'," +
                        "'username' = 'root'," +
                        "'password' = '123456'," +
                        "'database-name' = 'gmallplus'," +
                        "'table-name' = 'base_trademark'" +
                        ")")
                .print();

        env.execute();


    }
}
