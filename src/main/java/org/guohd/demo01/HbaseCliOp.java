package org.guohd.demo01;
/**
 * @author guohd created by
 * @date
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseCliOp {
    // .相关配置属性

    private Configuration conf;

    private HBaseAdmin admin;

    //    private HConnection connection; //habse1.x版本中已过期
    private HConnection connection;

    public HbaseCliOp(Configuration conf) throws IOException {

        this.conf = HBaseConfiguration.create(conf);

        this.admin = new HBaseAdmin(this.conf);

    }

    public HbaseCliOp() throws IOException {

        Configuration cnf = new Configuration();

        this.conf = HBaseConfiguration.create(cnf);

        this.admin = new HBaseAdmin(this.conf);

//        this.connection = HConnectionManager.createConnection(conf); //habse1.x版本中已过期
        // 通过ConnectionFactory 创建connection
        this.connection = HConnectionManager.createConnection(conf);


    }

    // 获取表名
    public HTableInterface getTable(String tableName) {

        try {
//            return connection.getTable(tableName);
            return this.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }
    // 1.创建表

    public void createTable(String tableName, String colFamilies[])
            throws IOException {

        if (this.admin.tableExists(tableName)) {

            System.out.println("Table: " + tableName + " already exists !");

        } else {

            HTableDescriptor dsc = new HTableDescriptor(TableName.valueOf(tableName));
                    //this.admin.getTableDescriptor(TableName.valueOf(tableName));
            int len = colFamilies.length;

            for (int i = 0; i < len; i++) {

                HColumnDescriptor family = new HColumnDescriptor(colFamilies[i]);
                //将表放到rs缓存中，保证在读取的时候被命中
                family.setInMemory(true);
                //设置数据存储生命周期
//                family.setTimeToLive()
                dsc.addFamily(family);

            }
            admin.createTable(dsc);
            System.out.println("创建表成功");

        }

    }

    // 2.删除表
    public void deleteTable(String tableName) throws IOException {

        if (this.admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("删除表成功");

        } else {

            System.out.println("Table Not Exists !");

        }

    }


    // 3.插入一行记录

    public void insertRecord(String tableName, String rowkey, String family,
                             String qualifier, String value) throws IOException {

        HTableInterface table = this.getTable(tableName);

        Put put = new Put(rowkey.getBytes());

        put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());

        table.put(put);
        System.out.println("插入一行数据成功");

        table.close();

    }

    // 批量插入：一次写入多少条
    public void insertRecords(String tableName, String rowkey, String familys[],
                              String qualifier, String value, int sizes) throws IOException {

        HTableInterface table = this.getTable(tableName);
        List<Put> puts = new ArrayList<Put>();
        Put put = null;
        for (int i = 0; i < sizes; i++) {
            put = new Put(Bytes.toBytes(rowkey + "_" + i));
            for (String family : familys) {
                put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
            puts.add(put);
        }

        table.put(puts);
        System.out.println("批量插入数据成功");

    }

    // 4.删除一行记录

    public void deleteRecord(String tableName, String rowkey)
            throws IOException {

        HTableInterface table = this.getTable(tableName);

        Delete del = new Delete(rowkey.getBytes());

        table.delete(del);

        System.out.println("删除行成功");

    }

    // 清空表记录
    public void truncateTable(String tableName) {
    }

    // 5.获取一行记录:根据rowkey获取

    public Result getByRowKey(String tableName, String rowkey)
            throws IOException {

        HTableInterface table = this.getTable(tableName);

        Get get = new Get(rowkey.getBytes());

        Result rs = table.get(get);

        return rs;

    }


    //批量获取
    public Result[] getBatchs(String tableName, int size, String rowkey[], String cf[], String qualifier[]) {
        HTableInterface table = this.getTable(tableName);
        List<Get> gets = new ArrayList<Get>();
        Get get = null;

        try {
            for (int i = 0; i < size; i++) {
                get = new Get(Bytes.toBytes(rowkey[i]));
                get.addColumn(Bytes.toBytes(cf[i]), Bytes.toBytes(qualifier[i]));
            }
            gets.add(get);
            return table.get(gets);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 6.获取所有记录

    public List<Result> scanRecord(String tableName) throws IOException {

        HTableInterface table = this.getTable(tableName);

        Scan scan = new Scan();
        scan.setCacheBlocks(true);
        ResultScanner scanner = table.getScanner(scan);
        scan.setStartRow(Bytes.toBytes("1"));
        scan.setStopRow(Bytes.toBytes("100000"));
        List<Result> list = new ArrayList<Result>();
        for (Result r : scanner) {

            list.add(r);
        }

        scanner.close();
        return list;

    }

    public void close() {
        //关闭资源
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
