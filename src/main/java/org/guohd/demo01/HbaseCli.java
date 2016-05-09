package org.guohd.demo01;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * @author izhonghong
 *         表user01
 */

public class HbaseCli {

    public static void main(String[] args) {

        try {
            HbaseUtils hbaseOp = new HbaseUtils();
            String[] familys = {"info1", "info2"};
//		hbaseOp.createTable("user01", familys);
            System.out.println(new Date());
            hbaseOp.insertRecords("user01", "row", familys, "id", UUID.randomUUID().toString(), 100);
            System.out.println(new Date());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
