package org.guohd.demo02;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MutilInsert implements Runnable {
    public static int n=0;
    static int size = HbaseModel.SIZE/HbaseModel.THREAD_NUM;
    @Override
    public void run() {
            try {
                insert(HbaseModel.TABLE_NAME, size);
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
    private static void insert(String tableName, int sizes) throws Exception {
        HTableInterface table = HbaseModel.getConnection().getTable(TableName.valueOf(tableName));

        table.setAutoFlush(false, false);
        table.setWriteBufferSize(512 * 1024);

        // 构造测试数据
        List<Put> puts = new ArrayList<Put>();
        Random rand = new Random();
        for (int i = 1; i <= sizes; i++) {

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
            Put put = new Put(String.format("%d",n).getBytes());
            Cell[] kv = new Cell[2];
            kv[0] = new KeyValue(String.format("%d",n).getBytes(),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("info"),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("count"),
                    String.format("%d", rand.nextInt(500)).getBytes());
            kv[1] = new KeyValue(String.format("%d",n).getBytes(),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("info"),
                    org.apache.hadoop.hbase.util.Bytes.toBytes("avgts"),
                    String.format("%f", rand.nextDouble()).getBytes());

            for (Cell cell : kv) {
                put.add(cell);
            }
            puts.add(put);
            if(i % sizes == 0) {
//                System.out.println(puts.size());
                table.put(puts);
                puts.clear();
//                table.flushCommits();
            }
            n++;
        }
        table.close();
    }

    /**
     * 多线程插入.
     */
    public static void mutilInsert(int threadNum)throws Exception{
        System.out.println("#############多线程插入开始##################");
        long start = System.currentTimeMillis();
        Runnable r = new MutilInsert();
        Thread ts [] = new Thread[threadNum];
        for (int i = 0; i<ts.length; i++){
            ts[i] = new Thread(r);
            ts[i].start();
            ts[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println("多线程插入"+HbaseModel.SIZE+"条耗时"+(end-start)/1000+"s");
        System.out.println("#############多线程插入结束##################");

    }


}
