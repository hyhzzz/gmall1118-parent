package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DruidDSUtil;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author coderhyh
 * @create 2022-08-16 8:25
 * 从主流中过滤维度数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private PreparedStatement ps;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
     /*   //注册驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);

        //获取连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_URL);*/

        //获取连接池对象
        druidDataSource = DruidDSUtil.createDataSource();

    }


    //处理主流数据
    @Override
    public void processElement(JSONObject jsonObj,
                               BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx,
                               Collector<JSONObject> out) throws Exception {

        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //根据处理业务流数据的表名，到广播中获取配置对象。
        String tableName = jsonObj.getString("table");

        TableProcess tableProcess = broadcastState.get(tableName);
        if (tableProcess != null) {
            //获取当前操作影响的业务记录
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");

            //在向下游传递数据前，将不需要传递的属性过滤掉
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(dataJsonObj, sinkColumns);

            //在向下游传递数据前，应该补充维度发送的目的地 sink_table
            String sinkTable = tableProcess.getSinkTable();
            dataJsonObj.put("sink_table", sinkTable);

            //如果获取到了，说明当前处理的业务流数据是维度
            out.collect(dataJsonObj);

        } else {
            //从广播状态(配置表中)没有获取到配置信息，说明不是维度,是事实表
            System.out.println("不是维度数据，过滤掉" + jsonObj);
        }
    }

    /*
     *配置流的数据格式
     * {"before":null,
     * "after":{"source_table":"activity_info",
     * "sink_table":"dim_activity_info",
     * "sink_columns":"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time",
     * "sink_pk":"id",
     * "sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source", "ts_ms":1660579797225,"snapshot":"false","db":"gmall1118-realtime","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1660579797229,"transaction":null}
     * */
    //处理广播(配置流)数据
    @Override
    public void processBroadcastElement(String jsonStr,
                                        BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx,
                                        Collector<JSONObject> out) throws Exception {

        //为了处理方便，可以将从配置表中读取的一条数据由jsonStr转换为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);

        //获取配置表中的一条记录
        //        JSONObject afterJsonObj = jsonObj.getJSONObject("after");
        //将读取到的配置信息，封装为tableprocess对象
        //        TableProcess tableProcess = afterJsonObj.toJavaObject(TableProcess.class);

        TableProcess tableProcess = jsonObj.getObject("after", TableProcess.class);

        //获取当前配置相关的属性
        //获取业务数据库表名
        String sourceTable = tableProcess.getSourceTable();
        //获取输出中维度表名
        String sinkTable = tableProcess.getSinkTable();
        //获取数仓中维度表中的字段
        String sinkColumns = tableProcess.getSinkColumns();
        //获取数仓中维度表中的主键
        String sinkPk = tableProcess.getSinkPk();
        //获取建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();


        //提前将维度表创建出来
        //create table is not exists 表空间.表名(字段名称 字段类型 primary key,字段名称 字段类型 ....)Extend;
        checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //将配置表的中的配置信息放到广播状态中保存起来
        broadcastState.put(sourceTable, tableProcess);
    }

    private void checkTable(String tableName, String columnStr, String pk, String ext) {

        //对空值进行处理
        if (ext == null) {
            ext = "";
        }
        if (pk == null) {
            pk = "id";
        }

        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + tableName + "(");

        String[] columnArr = columnStr.split(",");
        for (int i = 0; i < columnArr.length; i++) {
            String column = columnArr[i];
            if (column.equals(pk)) {
                createSql.append(column + " varchar primary key");
            } else {
                createSql.append(column + " varchar");
            }
            //除了最后一个字段后面不加逗号，其它的字段都需要在后面加一个逗号
            if (i < columnArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(") " + ext);
        System.out.println("在phoenix中执行的建表语句: " + createSql);


        //执行建表语句
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            //从连接池中获取连接对象
            conn = druidDataSource.getConnection();
            //创建数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行sql语句  execute:ddl  executeupdate：dml  executequery：dql
            ps.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            //释放资源
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    //过滤字段  dataJsonObj: {"tm_name":"xzls11","logo_url":"dfas","id":12}
    //sinkColumns:  id,tm_name
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fieldArr);
        //获取json对象中所有的元素
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !fieldList.contains(entry.getKey()));
    }
}
