package org.guohd.demo02;

public class SingleInsert {
    /*
     * 单线程插入数据
     */
    public static void singleThreadInsert(String tableName, int sizes)
            throws Exception {

        System.out.println("---------开始SingleThreadInsert测试----------");
        long start = System.currentTimeMillis();

        HbaseUtils.insert(tableName, HbaseModel.SIZE);

        long stop = System.currentTimeMillis();

        System.out.println("插入" + sizes + "数据共耗时：" + (stop - start) * 1.0
                / 1000 + "s");
        System.out.println("---------结束SingleThreadInsert测试----------");

    }

    public SingleInsert() {
        super();
    }


}