package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 设置流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境并行度根据kafka的分区数设置 最好一样 不一样也行

        //TODO 状态后端 生产环境下设置
/*        env.setStateBackend(new FsStateBackend("hdfs://"))
        //开启CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);*/


        //TODO 读取上层Kafka的ods_base_log的数据
        String topic = "ods_base_log";
        String groupId = "ods_dwd_base_log_app";
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        //TODO 判断是否JSON类型 过滤脏数据
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty") {};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyOutputTag, s);
                }
            }
        });

        jsonObjDS.getSideOutput(dirtyOutputTag).print("Dirty");

        //TODO 新老用户校验 按照Mid分组 状态编程
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            //初始化 每个并行度执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");

                if ("1".equals(isNew)) {

                    //取出状态数据
                    String state = valueState.value();

                    if (state != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        valueState.update("0");
                    }
                }
                return value;
            }
        });

        //TODO 分流 使用侧输出流  启动、页面、曝光
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};
        OutputTag<String> displayOutputTag = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {

                //获取启动数据
                String start = value.getString("start");
                if (start != null && start.length() > 2) {
                    //启动日志,将数据写入启动日志侧输出流
                    context.output(startOutputTag, value.toJSONString());
                } else {

                    //非启动就是页面 写入主流
                    collector.collect(value.toJSONString());

                    //曝光数据包含在页面数据里
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        //提取页面ID 加入到display中
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //单个display信息太少  生产环境可以依据情况添加
                            display.put("page_id", pageId);

                            //将数据写入曝光侧输出流
                            context.output(displayOutputTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 将3个流写入对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);

        startDS.print("Start>>>>>>>>");
        pageDS.print("Page>>>>>>>>");
        displayDS.print("Display>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 执行任务
        env.execute();

    }
}
