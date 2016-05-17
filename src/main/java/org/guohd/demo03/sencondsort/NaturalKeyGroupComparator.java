package org.guohd.demo03.sencondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by guohd on 2016/5/17.
 * 自定义分组比较器，将count相同的放到一组.
 */
public class NaturalKeyGroupComparator extends WritableComparator{
    public NaturalKeyGroupComparator(){
        super(SortKeyPair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        SortKeyPair s1 = (SortKeyPair)a;
        SortKeyPair s2 = (SortKeyPair)b;

        int res = s1.getCount()<s2.getCount()?1:(s1.getCount() == s2.getCount()?0:-1);

        return res;
    }
}
