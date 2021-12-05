package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    @SneakyThrows
    public static void main(String[] args) throws Exception {

        //TODO 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

    /*
        //TODO 设置状态后端
        env.setStateBackend(new FsStateBackend(""));
        //设置CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        */

        //TODO 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE ORDER_WIDE (\n" +
                        "  `province_id` BIGINT,\n" +
                        "  `province_name` STRING,\n" +
                        "  `province_area_code` STRING,\n" +
                        "  `province_iso_code` STRING,\n" +
                        "  `province_3166_2_code` STRING,\n" +
                        "  `order_id` STRING,\n" +
                        "  `split_total_amount` Decimal,\n" +
                        "  `create_time` STRING,\n" +
                        "  rt as TO_TIMESTAMP(create_time),\n" +
                        "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n" +
                        ") WITH (" + MyKafkaUtil.getKafkaDDL("dwm_order_wide", "province_stats_sql_app") +")"
        );

        //TODO 分组开窗聚合  查询
        Table table = tableEnv.sqlQuery(
                "Select\n" +
                        "  DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                        "  DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                        "  province_id,\n" +
                        "  province_name,\n" +
                        "  province_area_code,\n" +
                        "  province_iso_code,\n" +
                        "  province_3166_2_code,\n" +
                        "  count(distinct order_id) order_count,\n" +
                        "  sum(split_total_amount) order_amount,\n" +
                        "  UNIX_TIMESTAMP()*1000 ts\n" +  //在ClickHouse的作用是提供版本控制
                        "from\n" +
                        "  ORDER_WIDE\n" +
                        "GROUP BY\n" +
                        " province_id,\n" +
                        " province_name,\n" +
                        " province_area_code,\n" +
                        " province_iso_code,\n" +
                        " province_3166_2_code,\n" +
                        " TUMBLE(rt, INTERVAL '10' SECOND)"
        );


        //TODO 4.转换为流 //用追加流的原因是我们开窗了 在窗口内聚合就是加了GroupBy 最终在窗口中也只输出一条
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(table, ProvinceStats.class);

        //TODO 5.将数据写出到ClickHouse
        provinceStatsDS.print();
        //ClickHouse中的表字段可以和JavaBean中的不一样 这里是按照顺序写入
        provinceStatsDS.addSink(ClickHouseUtil.getSinkFunction("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动任务
        env.execute();

    }
}
