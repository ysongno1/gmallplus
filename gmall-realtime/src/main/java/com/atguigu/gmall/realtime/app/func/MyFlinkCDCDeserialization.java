package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyFlinkCDCDeserialization implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception{

        //TODO 获取数据库名表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String tableName = split[2];

        //TODO 获取数据
        Struct value = (Struct) sourceRecord.value();

        //获取value里after所在数据的struct
        Struct afterStruct = value.getStruct("after");
        //创建用来存放after数据的JSONObject
        JSONObject afterJSON = new JSONObject();
        if (afterStruct != null) {
            for (Field field : afterStruct.schema().fields()) {
                afterJSON.put(field.name(), afterStruct.get(field));
            }
        }

        //获取value里before所在数据的struct
        Struct beforeStruct = value.getStruct("after");
        //创建用来存放before数据的JSONObject
        JSONObject beforeJSON = new JSONObject();
        if (beforeStruct != null) {
            for (Field field : beforeStruct.schema().fields()) {
                beforeJSON.put(field.name(), beforeStruct.get(field));
            }
        }

        //TODO 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //因为在FlinkCDC里面数据的插入是create，我们需要更改他为insert
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type = "insert";
        }

        //TODO 创建用来存放结果的JSONObject
        JSONObject result = new JSONObject();
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("after", afterJSON);
        result.put("type", type);

        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }

}

