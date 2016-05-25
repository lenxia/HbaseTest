package org.guohd.demo01;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.guohd.demo02.HbaseModel;
import org.guohd.demo02.MutilInsert;
import org.guohd.demo02.SingleInsert;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Random;

/**
 * Created by izhonghong on 2016/5/6.
 * junit 测试habse用例
 */
public class TestCase {


    @Test
    public void testDB() {

    }

    @Test
    public void testRand() {
        DecimalFormat format = new DecimalFormat("######0.00");

        System.out.println(format.format(new Random().nextDouble()));
    }

    @Test
    public void testQuery() {
//        System.setProperty("host.name","hbase");


        try {
            long start = System.currentTimeMillis();
            HbaseCliOp cli = new HbaseCliOp();
            List<Result> list = cli.scanRecord("comment");
            System.out.println("row\t"+"col\t"+"value\t");
            for(Result rs : list){
                for (Cell c:rs.rawCells()){
                    System.out.println(Bytes.toString(c.getRowArray()));
                }
            }

            long end = System.currentTimeMillis();
            System.out.println((end-start)/1000);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testCreate()throws  Exception{
        String cf[]={"info"};
        new HbaseCliOp().createTable("test11",cf);
    }

    @Test
    public void testDel()throws  Exception{

        new HbaseCliOp().deleteTable("test11");
    }
    @Test
    public void testInsert() {
        try {
            new HbaseCliOp().insertRecord("test", "row1", "info", "count", "30");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSingleInsert() {

        try {
            SingleInsert.singleThreadInsert(HbaseModel.TABLE_NAME, HbaseModel.SIZE);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMutilInsert() {
        try {
            MutilInsert.mutilInsert(HbaseModel.THREAD_NUM);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
