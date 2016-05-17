package org.guohd.demo03.sencondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by izhonhhong on 2016/5/17.
 * 自定义组合key.
 * 要求key进行排序，需要实现WritableComparable接口重写排序规则.
 */
public class SortKeyPair implements WritableComparable<SortKeyPair> {

    private int count = 0;
    private long avgs = 0;

    //要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
    public SortKeyPair() {
    }

    public SortKeyPair(int count, long avgs) {
        super();
        this.count = count;
        this.avgs = avgs;
    }

    /*
    排序规则

     */
    @Override
    public int compareTo(SortKeyPair o) {
        //java中比较默认小的放在前面，即返回-1的放在前面，这里将小的放在后面，即返回1
        int res = this.count < o.getCount() ? 1 : (this.count == o.getCount() ? 0 : -1);

        //当count相同的时候，根据avg进行排序
        if (res == 0) {
            res = this.avgs < o.getAvgs() ? 1 : (this.avgs == o.getAvgs() ? 0 : -1);
        }

        return res;
    }

    //写出，作为下次的输入
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.count);
        dataOutput.writeLong(this.avgs);

    }

    //读入
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count = dataInput.readInt();
        this.avgs = dataInput.readLong();

    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setAvgs(long avgs) {
        this.avgs = avgs;
    }

    public int getCount() {
        return count;
    }

    public long getAvgs() {
        return avgs;
    }
}
