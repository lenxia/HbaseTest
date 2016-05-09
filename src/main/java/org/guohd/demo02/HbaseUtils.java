package org.guohd.demo02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * hbase工具类
 */
public class HbaseUtils extends HbaseInsert {

    public static void insert(String tableName,int sizes) throws IOException {
        Table table = HbaseProps.getConnection().getTable(TableName.valueOf(tableName));
//		table.setAutoFlush(false);
//        table.setWriteBufferSize(24 * 1024 * 1024);
        // 构造测试数据
        List<Put> list = new ArrayList<Put>();
//        byte[] buffer = new byte[350];
//        Random rand = new Random();
        for (int i = 0; i < sizes; i++) {
            Put put = new Put(String.format("%d", i).getBytes());
//            rand.nextBytes(buffer);
            Cell[] kv =null;
            kv[0] = new KeyValue(String.format("%d", i).getBytes(),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("info"),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("name"),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("zhonghong-"+i));
            kv[1] = new KeyValue(String.format("%d", i).getBytes(),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("info"),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("sex"),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("male"));
            for(Cell cell : kv){
                put.add(cell);
            }
//			put.add("info".getBytes(), null, buffer);
            // wal=false
            list.add(put);
            if (i % sizes == 0) {
                table.put(list);
                list.clear();
            }
        }
    }


}
