package org.guohd.demo04;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.guohd.demo01.HbaseCliOp;
import org.guohd.demo02.HbaseModel;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by guohd on 2016/5/24.
 */
public class ImportData {
    private static String url = "jdbc:mysql://192.168.2.4:3306/weibo_dc";
    private static String driver = "com.mysql.jdbc.Driver";
    private static String username = "dev";
    private static String password = "dev";
    private static Connection conn = null;
    private static PreparedStatement st = null;
    private static ResultSet rs = null;
    private static String sql = "select * from t_status_weibo_001 limit 100000";

    public static ResultSet getResult() {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            st = conn.prepareStatement(sql);
            rs = st.executeQuery();

            return rs;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void insert(String tableName) throws Exception {
        //mysql
        ResultSet resultSet = getResult();
        //hbase
        HTableInterface table = HbaseModel.getConnection().getTable(TableName.valueOf(tableName));
        table.setAutoFlush(false, false);
        table.setWriteBufferSize(512 * 1024);

        // 构造测试数据
        List<Put> puts = new ArrayList<Put>();

        /*
            String.format()用法
            1、转换符
            %s: 字符串类型，如："ljq"
            %b: 布尔类型，如：true
            %d: 整数类型(十进制)，如：99
            %f: 浮点类型，如：99.99
            %%: 百分比类型，如：％
            %n: 换行符
        */
        while (resultSet.next()) {
            Put put = new Put(String.format("%d", resultSet.getInt("id")).getBytes());
            Cell cells[] = new Cell[resultSet.getMetaData().getColumnCount()];
            for (int j = 1; j < cells.length; j++) {

                cells[j] = new KeyValue(String.format("%d", resultSet.getInt("id")).getBytes(),
                        org.apache.hadoop.hbase.util.Bytes.toBytes("info"),
                        org.apache.hadoop.hbase.util.Bytes.toBytes(resultSet.getMetaData().getColumnName(j + 1)),
                        String.format("%s", resultSet.getString(j + 1)).getBytes());
                put.add(cells[j]);
            }
            puts.add(put);


        }
        System.out.println("list:" + puts.size());
        table.put(puts);
        table.close();
        try {
            if (rs != null) rs.close();
            if (st != null) st.close();
            if (conn != null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void main(String args[]) throws Exception {
//        insert("t_status_weibo_001");
        Result rs = new HbaseCliOp().getByRowKey("t_status_weibo_001", "10005");
        byte b[] = rs.getValue(Bytes.toBytes("info"),Bytes.toBytes("text"));
        String text = new String(b,"UTF-8");
        System.out.println(text);

    }

}
