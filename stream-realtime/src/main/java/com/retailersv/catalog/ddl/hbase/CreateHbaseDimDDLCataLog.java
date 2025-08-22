package com.retailersv.catalog.ddl.hbase;

import com.stream.common.utils.ConfigUtils;
import com.stream.utils.HiveCatalogUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * 作用：
 * 1) 通过 Flink Table API 注册 HiveCatalog
 * 2) 使用 HBase SQL Connector (hbase-2.2) 创建维表 dim_base_dic 的外表
 * 3) 演示：显示表、删除旧表、创建新表、查询新表
 *
 * 说明：
 * - 该类只负责在 Catalog 中注册一张 HBase 外表，不做数据写入。
 * - 请确保 Flink 集群已加载对应版本的 hbase-connector 及 HBase/Hive 依赖。
 */
public class CreateHbaseDimDDLCataLog {
    // 从配置文件读取：HBase 命名空间
    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    // 从配置文件读取：ZooKeeper 集群地址（host:port,host:port,...）
    private static final String ZOOKEEPER_SERVER_HOST_LIST = ConfigUtils.getString("zookeeper.server.host.list");
    // 使用的 Flink HBase Connector 版本标识（与依赖一致）
    private static final String HBASE_CONNECTION_VERSION = "hbase-2.2";
    // DROP 语句前缀（便于复用）
    private static final String DROP_TABEL_PREFIX = "drop table if exists ";

    // 创建 HBase 外表的 DDL：
    // - 表名：hbase_dim_base_dic（注册到当前使用的 Catalog & Database）
    // - rowkey: rk
    // - 列族 info，包含 dic_name / parent_code 两列
    // - primary key (rk) not enforced：Upsert 语义，Catalog 层主键不强约束
    // - connector 配置：hbase-2.2, table-name, zookeeper.quorum
    private static final String createHbaseDimBaseDicDDL =
            "create table hbase_dim_base_dic (" +
                    "    rk string," +
                    "    info row<dic_name string, parent_code string>," +
                    "    primary key (rk) not enforced" +
                    ")" +
                    "with (" +
                    "    'connector' = '"+HBASE_CONNECTION_VERSION+"'," +
                    "    'table-name' = '"+HBASE_NAME_SPACE+":dim_base_dic'," +
                    "    'zookeeper.quorum' = '"+ZOOKEEPER_SERVER_HOST_LIST+"'" +
                    ")";

    public static void main(String[] args) {
        // 避免权限问题（按你的环境需要设置）
        System.setProperty("HADOOP_USER_NAME","root");

        // 1) 创建 Flink 流执行环境 & 表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 2) 构建并注册 HiveCatalog（名称：hive-catalog）
        HiveCatalog hiveCatalog = HiveCatalogUtils.getHiveCatalog("hive-catalog");
        tenv.registerCatalog("hive-catalog",hiveCatalog);

        // 3) 切换到 HiveCatalog（后续的 DDL/DML 都作用在该 Catalog）
        tenv.useCatalog("hive-catalog");

        // 4) 查看当前库下已有表（调试用）
        tenv.executeSql("show tables;").print();

        // 5) 先删表再建表，避免重复（从 create DDL 中解析表名）
        tenv.executeSql(DROP_TABEL_PREFIX + getCreateTableDDLTableName(createHbaseDimBaseDicDDL));

        // 6) 再次查看表（确认已删除）
        tenv.executeSql("show tables;").print();

        // 7) 创建 HBase 外表
        tenv.executeSql(createHbaseDimBaseDicDDL).print();

        // 8) 再查看一次表（确认已创建）
        tenv.executeSql("show tables;").print();

        // 9) 试读该表（若无数据会返回空结果；确保 HBase 里存在 ns:dim_base_dic 表与列族/列）
        tenv.executeSql("select * from hbase_dim_base_dic").print();
    }

    /**
     * 从一条标准的 CREATE TABLE DDL 中解析出表名。
     * 假设 DDL 形如：create table <tableName> ( ... ) with (...)
     * 简单 split 方式对格式较敏感，请确保 DDL 关键字与空格规范。
     */
    public static String getCreateTableDDLTableName(String createDDL){
        return createDDL.split(" ")[2].trim();
    }
}
