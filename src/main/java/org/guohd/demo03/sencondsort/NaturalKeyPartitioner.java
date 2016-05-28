package org.guohd.demo03.sencondsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by izhonhhong on 2016/5/17.
 * 自定义分区.
 * 如果采用取模hash，会造成各个reducer的结果顺序无法控制，因此我们不能用HashPartitioner。
 * 而自己的Partitioner则需要只按count进行partition，不管平均评价时间.
 */
public class NaturalKeyPartitioner extends Partitioner<SortKeyPair, Text> {

    @Override
    public int getPartition(SortKeyPair sortKeyPair, Text text, int i) {
        int count = sortKeyPair.getZan_count();

        if (count <= 5) {
            return 0;
        } else if (count > 5 && count <= 10) {
            return 1;
        } else if (count > 10 && count <= 15) {
            return 2;
        } else {
            return 3;

        }
    }
}
