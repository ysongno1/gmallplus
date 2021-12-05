package com.atguigu;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

public class Flink_CDCWithCustomerDeser {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建Flink-MySQL-CDC的Source
        Properties properties = new Properties();

        DebeziumSourceFunction<String> mysqlSource = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmallplus")
                .tableList("gmallplus.base_trademark")
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据,注意：指定的时候需要使用"db.table"的方式
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.initial())
                .deserializer(new DebeziumDeserializationSchema<String>() {

                    /**
                     *{
                     *   "database":"gmall_flink_0625",
                     *   "tableName":"aaa",
                     *   "after":{"id":"123","name":"zs"....},
                     *   "before":{"id":"123","name":"zs"....},
                     *   "type":"insert",
                     *}
                     */

                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        //TODO 1.创建用来存放结果的JSONObject
                        JSONObject result = new JSONObject();

                        //TODO 2. 获取数据库名及表名
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String database = split[1];
                        String tableName = split[2];

                        //TODO 3. 获取数据
                        Struct value = (Struct) sourceRecord.value();
                        //获取after数据
                        Struct afterStruct = value.getStruct("after");
                        //创建一个存放after修改之后的数据的JSONObject
                        JSONObject afterJson = new JSONObject();
                        if (afterStruct!=null){
                            for (Field field : afterStruct.schema().fields()) {
                                afterJson.put(field.name(), afterStruct.get(field));
                            }
                        }

                        //获取before数据
                        Struct beforeStruct = value.getStruct("before");
                        //创建一个存放after修改之后的数据的JSONObject
                        JSONObject beforeJson = new JSONObject();
                        if (beforeStruct!=null){
                            for (Field field : beforeStruct.schema().fields()) {
                                beforeJson.put(field.name(), beforeStruct.get(field));
                            }
                        }

                        //TODO 4.获取操作类型DETLETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //因为在CDC里面插入类型显示为create 我们把他更换成insert
                        String type = operation.toString().toLowerCase();
                        if ("create".equals(type)){
                            type = "insert";
                        }

                        //TODO 5.封装数据
                        result.put("database", database);
                        result.put("tableName", tableName);
                        result.put("after", afterJson);
                        result.put("before", beforeJson);
                        result.put("type", type);

                        collector.collect(result.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();

        //3.使用CDC Source从MySQL读取数据
        env.addSource(mysqlSource)
                .print();

        //4.执行任务
        env.execute();

    }
}
