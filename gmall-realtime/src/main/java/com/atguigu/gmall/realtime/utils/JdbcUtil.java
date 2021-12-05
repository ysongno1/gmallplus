package com.atguigu.gmall.realtime.utils;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/*
    只要能通过JDBC访问的 这个工具类都可用来查询
 */

public class JdbcUtil {

    /**
     *
     * @param connection 通过外部传参我们可以获得想要的某个框架的连接(Hbase,Mysql,Hive...)
     * @param sql 查询语句
     * @param clz 指定T这个类的类型
     * @param underScoreToCamel mysql字段下划线分割命名 Javabean是小驼峰命名
     *                          我们以前用的JSON.parseObject(value,class)转化格式时JSON的工具类为我们做了驼峰和下划线转化
     *                          我们需要自己完成这个转化工作
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(Connection connection,String sql, Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        //创建集合用于存放查询结果对象
        ArrayList<T> result = new ArrayList<>();

        //编译sql语句
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //因为SQL语句不确定 所以查询的列也不确定  从元数据获取列的个数
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历resultSet，对每行数据封装T对象 并将T对象添加至集合
        while (resultSet.next()){

            //创建一个T类型的对象
            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1; i++) { //这里的角标是从1开始
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);

                //将数据库的表名转化为JavaBean的格式
                if (underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                //利用BeanUtils将数据写入t对象
                BeanUtils.setProperty(t,columnName, value);
            }
            //循环结束 一行数据封装完成
            result.add(t);
        }

        //返回结果集
        return result;

    }
}
