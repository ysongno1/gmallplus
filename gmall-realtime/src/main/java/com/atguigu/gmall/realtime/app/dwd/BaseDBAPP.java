package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.MyFlinkCDCDeserialization;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/*
    数据流：mysql > >ods_base_db > hbase(dim)/kafka(dwd)
    程序：mysql hdfs zk hbase phoenix kafka
 */

public class BaseDBAPP {
    public static void main(String[] args) throws Exception {

        //TODO 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*
        //生产环境下设置状态后端
        env.setParallelism(1);
        //开启CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
*/

        //TODO 消费ods_base_db上的业务数据
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "base_db_app"));

        //TODO 转换为JSON对象 并过滤脏数据
        OutputTag<String> drityOutputTag = new OutputTag<String>("drity"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(drityOutputTag, s);
                }
            }
        });
        jsonObjDS.getSideOutput(drityOutputTag).print("Dirty>>>>>>");

        //TODO 过滤掉type类型为delete的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return !"delete".equals(value.getString("type"));

            }
        });

        //TODO 使用FlinkCDC读取MySQL配置表并创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmallplus_realtime")
                .tableList("gmallplus_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDCDeserialization())
                .build();

        DataStreamSource<String> cdcDS = env.addSource(sourceFunction);

//        cdcDS.print("cdcDS>>>");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("broad", String.class, TableProcess.class);
        BroadcastStream<String> broadDS = cdcDS.broadcast(mapStateDescriptor);



        //TODO 连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadDS);

        //TODO 根据配置流信息处理主流数据（分流）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = connectDS.process(new TableProcessFunction(mapStateDescriptor, hbaseTag));

        //TODO 将HBase数据写出
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        hbaseDS.print("hbase>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        //TODO 将Kafka数据写出
        kafkaDS.print("kafka>>>>>>");
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sink_table"), jsonObject.getString("after").getBytes(StandardCharsets.UTF_8));
            }
        }));

        //TODO 启动任务
        env.execute("BaseDBAPP");
    }
}
