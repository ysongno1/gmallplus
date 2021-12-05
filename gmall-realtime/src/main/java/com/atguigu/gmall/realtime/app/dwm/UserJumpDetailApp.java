package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {

        //TODO 获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*
        //TODO 开启状态后端
        env.setStateBackend(new FsStateBackend(""));
        //开启CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
*/

        //TODO 读取dwd_page_log
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "user_jump_detail_app"));

        //TODO 转换为JSON
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjWithWMDS = kafkaDS
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            ctx.output(dirtyTag, value);
                        }
                    }
                })
                /*
                	waterMark是广播出去的 跟keyby没有关系
	                waterMark表示小于(waterMark-乱序时间) 的事件全部到齐
                 */
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));
        jsonObjWithWMDS.getSideOutput(dirtyTag).print("dirty>>>>>>");

        //TODO 根据mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWMDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        //TODO CEP 定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageID = value.getJSONObject("page").getString("last_page_id");
                        return lastPageID == null || lastPageID.length() <= 0;
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(10));

        //TODO 把模式作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 挑选出匹配上的数据
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream
                .select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("begin").get(0).toJSONString();
                    }
                }, new RichPatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("begin").get(0).toJSONString();
                    }
                });

        //TODO 合并超时的和单跳的
        DataStream<String> timeoutDS = selectDS.getSideOutput(timeOutTag);
        timeoutDS.print("timeOut>>>>>>");
        selectDS.print("select>>>>>>>");

        DataStream<String> unionDS = selectDS.union(timeoutDS);
        unionDS.print("union>>>>>>");
        unionDS.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));


        //TODO 执行任务
        env.execute();

    }
}
