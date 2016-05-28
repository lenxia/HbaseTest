package org.guohd.demo03.sencondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by izhonhhong on 2016/5/17.
 * 定义比较器.
 */
public class CompositeKeyComparator extends WritableComparator {

    //通知CompositeKeyComparator使用SortKeyPair
    public CompositeKeyComparator() {
        super(SortKeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SortKeyPair s1 = (SortKeyPair) a;
        SortKeyPair s2 = (SortKeyPair) b;
        return s1.compareTo(s2);
    }
}

