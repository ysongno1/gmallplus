package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*
        //生产环境下使用
        //设置状态后端
        env.setStateBackend(new FsStateBackend(""));
        //开启ck (精准一次性)
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
*/

        //TODO 获取Kafka上dwd_page_log
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "unique_visit_app"));

        //TODO 将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> JsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        JsonObjDS.getSideOutput(dirtyTag).print("Dirty>>>>>>");

        //TODO 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = JsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        //TODO 使用状态编程对每天的mid做去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date—state", String.class);
                //状态值主要用于筛选是否今天来过,设置状态24小时后过期 避免状态过大
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //取last_page_id
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                //上个页面为空 证明用户刚进入我们的APP
                if (lastPageId == null) {

                    //获取状态中的数据
                    String state = dateState.value();

                    //取出当前数据的日期
                    String curDate = sdf.format(value.getLong("ts"));

                    if (state == null || !state.equals(curDate)) {
                        //更新状态
                        dateState.update(curDate);
                        return true;
                    }
                }
                //上个页面不为空 状态不为空&&状态里面的日期不是今天
                return false;
            }
        });


        //TODO 将数据写入Kafka
        filterDS.print("uv>>>>>>");
        filterDS.map(JSON::toString)
                .addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        //TODO 启动任务
        env.execute();
    }
}
