package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 获取流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*
        //TODO 获取状态后端
        env.setStateBackend(new FsStateBackend(""));
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
*/

        //TODO 获取上一层Kafka的数据
        String groupId = "visitor_stats_app";
        DataStreamSource<String> pageKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", groupId));
        DataStreamSource<String> uvKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwm_unique_visit", groupId));
        DataStreamSource<String> ujKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwm_user_jump_detail", groupId));

        //TODO 转化为JavaBean
        SingleOutputStreamOperator<VisitorStats> pageDS = pageKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    page.getString("last_page_id") == null ? 1L : 0L,
                    0L,
                    page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> uvDS = uvKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> ujDS = ujKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //TODO union三个流
        DataStream<VisitorStats> unionDS = pageDS.union(uvDS, ujDS);

        //TODO 提取事件时间生成waterMark
        SingleOutputStreamOperator<VisitorStats> unionWithWMDS = unionDS
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));


        //TODO 分组聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = unionWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return new Tuple4<>(
                                value.getVc(),
                                value.getCh(),
                                value.getAr(),
                                value.getIs_new()
                        );
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));


        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        return value1;
                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                        //取出数据
                        VisitorStats visitorStats = input.iterator().next();

                        visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        out.collect(visitorStats);
                    }
                });

        //TODO 将数据写出到ClickHouse
        reduceDS.print();
        reduceDS.addSink(ClickHouseUtil.getSinkFunction("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 启动任务
        env.execute("VisitorStatsApp");

    }
}
