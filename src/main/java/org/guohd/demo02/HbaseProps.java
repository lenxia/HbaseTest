package org.guohd.demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableUtil;

import java.io.IOException;

/**
 * 初始化hbase参数
 *
 * @author guohd
 */
public class HbaseProps {
    static Configuration hbaseConfig = null;
//    private static Connection connection = null;
    public final static String TABLE_NAME = "t_user";
    private static HConnection connection = null;




    static {
        // conf = HBaseConfiguration.create();
        Configuration HBASE_CONFIG = new Configuration();
        /*HBASE_CONFIG.set("hbase.master", "192.168.2.11:60000");
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.2.11");
		HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");*/
        hbaseConfig = HBaseConfiguration.create(HBASE_CONFIG);
    }

    public static HConnection getConnection() {
        if (connection == null) {
            try {
                return HConnectionManager.createConnection(new Configuration());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}
