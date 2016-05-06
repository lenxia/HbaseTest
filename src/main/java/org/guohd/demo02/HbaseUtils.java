package org.guohd.demo02;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * hbase工具类
 */
public class HbaseUtils extends HbaseInsert {

	@SuppressWarnings("deprecation")
	public static void insert(String tableName,int count) throws IOException {
        Table table = HbaseProps.connection.getTable(TableName.valueOf(tableName));
//		table.setAutoFlush(false);
		table.setWriteBufferSize(24 * 1024 * 1024);
		// 构造测试数据
		List<Put> list = new ArrayList<Put>();
		byte[] buffer = new byte[350];
		Random rand = new Random();
		for (int i = 0; i < count; i++) {
			Put put = new Put(String.format("%d", i).getBytes());
			rand.nextBytes(buffer);
			put.add("info".getBytes(), null, buffer);
			// wal=false
			put.setWriteToWAL(false);
			list.add(put);

			if (i % count == 0) {
				table.put(list);
				list.clear();
			}
		}
	}
	
	
	
	

}
