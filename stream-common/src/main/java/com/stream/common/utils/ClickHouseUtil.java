//package com.stream.common.utils;
//
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//
//import java.lang.annotation.Retention;
//import java.lang.annotation.RetentionPolicy;
//import java.lang.reflect.Field;
//import java.sql.PreparedStatement;
//
///**
// * ClickHouse 工具类：提供通用 SinkFunction 和建表方法
// * @author Ghost
// */
//public class ClickHouseUtil {
//    @Retention(RetentionPolicy.RUNTIME)
//    public @interface TransientSink {
//    }
//    /**
//     * 获取通用 SinkFunction，用于写入 ClickHouse
//     * @param sql 插入 SQL（如：insert into table_name values (?, ?, ?...)）
//     * @param <T> 数据类型
//     * @return SinkFunction
//     */
//    public static <T> SinkFunction<T> getSink(String sql) {
//        return JdbcSink.sink(
//                sql,
//                new JdbcStatementBuilder<T>() {
//                    @Override
//                    public void accept(PreparedStatement ps, T t) {
//                        try {
//                            Field[] fields = t.getClass().getDeclaredFields();
//                            int offset = 0;
//                            for (int i = 0; i < fields.length; i++) {
//                                Field field = fields[i];
//                                field.setAccessible(true);
//
//                                // 如果有 @TransientSink 注解，则跳过
//                                TransientSink transientSink = field.getAnnotation(TransientSink.class);
//                                if (transientSink != null) {
//                                    offset++;
//                                    continue;
//                                }
//
//                                Object value = field.get(t);
//                                ps.setObject(i + 1 - offset, value);
//                            }
//                        } catch (Exception e) {
//                            throw new RuntimeException("ClickHouse sink set value failed!", e);
//                        }
//                    }
//                },
//                JdbcExecutionOptions.builder()
//                        .withBatchSize(5)
//                        .withBatchIntervalMs(1000)
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl(Constant.CLICKHOUSE_URL)
//                        .withDriverName(Constant.CLICKHOUSE_DRIVER)
//                        .withUsername("root")
//                        .withPassword("123456")
//                        .build()
//        );
//    }
//
//
//}
