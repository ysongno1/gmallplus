package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TransferQueue;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSinkFunction(String sql) {

        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //可以从外面传参获取t类型的字段  也可以用反射获取全部字段
                        //这里用反射 通过属性调对象
                        Class<?> clz = t.getClass();

                        //获取所有属性名
//                        Field[] fields = clz.getFields(); //这个只能获取Public属性字段
                        Field[] declaredFields = clz.getDeclaredFields();//可以获取public,protected,default(缺省),private的属性字段

                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {

                            //获取当前属性
                            Field field = declaredFields[i];
                            field.setAccessible(true);  //设置当前公有私有的属性都可以访问

                            //尝试获取字段上的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null){
                                //如果JavaBean上有字段被@Transient注解所标识 JavaBean字段和SQL里?号位置就会对应不上
                                //i--; //设置i--就进入死循环了
                                offset++; //引进一个变量 专门控制如果有@Transient的情况下角标的对应
                                continue;
                             }

                            //通过反射的方式获取当前属性值
                            Object value = field.get(t); //加注解的方式抛异常

                            //对预编译SQL对象赋值
                            preparedStatement.setObject(i + 1 - offset, value);

                        }


                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5) //做一个批处理 5条数据执行一次
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());

    }

}
