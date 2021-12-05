package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.Serde;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {

        //TODO 创建流环境
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

        //TODO 读取上一层Kafka里的数据
        DataStreamSource<String> paymentInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwd_payment_info", "payment_wide_app"));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("dwm_order_wide", "payment_wide_app"));

        //TODO 转化为JavaBean 并设置WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWMDS = paymentInfoKafkaDS
                .map(line -> {
                    PaymentInfo paymentInfo = JSON.parseObject(line, PaymentInfo.class);
                    return paymentInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    return recordTimestamp; //这条数据有问题的话不参与WaterMark的创建
                                }
                            }
                        }));

        SingleOutputStreamOperator<OrderWide> orderWideWithWMDS = orderWideKafkaDS
                .map(line -> {
                    OrderWide orderWide = JSON.parseObject(line, OrderWide.class);
                    return orderWide;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    return recordTimestamp; //这条数据有问题的话不参与WaterMark的创建
                                }
                            }
                        }));


        //TODO 双流Join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoWithWMDS.keyBy(line -> line.getOrder_id())
                .intervalJoin(orderWideWithWMDS.keyBy(line -> line.getOrder_id()))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 将数据写出到Kafka
        paymentWideDS.print("paymentWide>>>>>>");
        paymentWideDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink("dwm_payment_wide"));



        //TODO 执行任务
        env.execute();

    }
}
