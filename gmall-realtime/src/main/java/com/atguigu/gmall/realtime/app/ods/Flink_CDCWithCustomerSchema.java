package com.atguigu.gmall.realtime.app.ods;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.MyFlinkCDCDeserialization;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {

        //TODO 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 创建Flink-MySQL-CDC的Source
        DebeziumSourceFunction<String> mySQLSource = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmallplus")
//                .tableList("gmallplus.base_trademark","gmallplus.order_info")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyFlinkCDCDeserialization())
                .build();

        //TODO Source从MySQL读取数据
        DataStreamSource<String> streamSource = env.addSource(mySQLSource);

        streamSource.print();

        //TODO 将数据发送至Kafka的ods_base_db
        streamSource.addSink (MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute("Flink_CDCWithCustomerSchema");

    }
}
