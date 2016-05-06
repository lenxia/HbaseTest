package org.guohd.demo01;

import org.guohd.demo02.HbaseInsert;
import org.guohd.demo02.HbaseProps;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by izhonghong on 2016/5/6.
 * junit 测试habse用例
 */
public class TestCase {
    @Test
    public void testSingleInsert(){

        try {
            HbaseInsert.SingleThreadInsert(HbaseProps.TABLE_NAME,100000);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMutilInsert(){
        try {
            HbaseInsert.MultThreadInsert(HbaseProps.TABLE_NAME,100000);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
