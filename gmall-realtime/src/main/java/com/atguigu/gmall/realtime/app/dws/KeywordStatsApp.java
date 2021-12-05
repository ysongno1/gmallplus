package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.splitFunction;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
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

        //TODO 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 使用DDL方式创建动态表
        tableEnv.executeSql(
                "CREATE TABLE page_log(\n" +
                        "  page Map<String,String>,\n" +
                        "  ts BIGINT,\n" +
                        "  rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),\n" +
                        "  WATERMARK FOR rt as rt - INTERVAL '1' SECOND\n" +
                        ") WITH (" + MyKafkaUtil.getKafkaDDL("dwd_page_log", "keyword_stats_app") + ")"
        );

        //TODO 过滤字段 只需要搜索的日志
        Table filterTable = tableEnv.sqlQuery(
                "Select \n" +
                        "  page['item'] keyword,\n" +
                        "  rt\n" +
                        "from\n" +
                        "  page_log\n" +
                        "where\n" +
                        "  page['item_type'] = 'keyword'\n" +
                        "  and   page['item'] is not null"
        );

        //TODO 注册函数和过滤表
        tableEnv.createTemporaryFunction("splitFunction", splitFunction.class);
        tableEnv.createTemporaryView("filter_table", filterTable);

        //TODO 使用UDTF进行分词
        //未注册临时视图的写法
        //tableEnv.sqlQuery("SELECT word,rt FROM " + filterTable + ", LATERAL TABLE(SplitFunction(key_word))");
        Table wordTable = tableEnv.sqlQuery(
                "SELECT word,rt FROM filter_table, LATERAL TABLE(SplitFunction(keyword))"
        );

        //TODO 分组开窗聚合
        tableEnv.createTemporaryView("word_table", wordTable);
        Table resultTable = tableEnv.sqlQuery(
                "select " +
                        "    'search' source, " +
                        "    DATE_FORMAT(TUMBLE_START(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                        "    DATE_FORMAT(TUMBLE_END(rt,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                        "    word keyword, " +
                        "    count(*) ct, " +
                        "    UNIX_TIMESTAMP()*1000 ts " +
                        "from word_table " +
                        "group by word,TUMBLE(rt,INTERVAL '10' SECOND)"
        );

        //TODO 将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 将数据写入到ClickHouse
        //名称按照表名 顺序按照JavaBean
        keywordStatsDS.print(">>>>>>>");
        keywordStatsDS.addSink(ClickHouseUtil.getSinkFunction("insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 执行任务
        env.execute();
    }
}
