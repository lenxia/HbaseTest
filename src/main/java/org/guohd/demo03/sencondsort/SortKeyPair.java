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


    private int zan_count = 0;
    private int comments_count = 0;

    public void setZan_count(int zan_count) {
        this.zan_count = zan_count;
    }

    public void setComments_count(int comments_count) {
        this.comments_count = comments_count;
    }

    //要写一个默认构造函数，否则MapReduce的反射机制，无法创建该类报错
    public SortKeyPair() {
    }

    public SortKeyPair(int zan_count, int comments_count) {
        super();
        this.zan_count = zan_count;
        this.comments_count = comments_count;
    }

    /*
    排序规则

     */
    @Override
    public int compareTo(SortKeyPair o) {
//        //java中比较默认小的放在前面，即返回-1的放在前面，这里将小的放在后面，即返回1
//        int res = this.zan_count < o.getZan_count() ? 1 : (this.zan_count == o.getZan_count() ? 0 : -1);
//
//        //当count相同的时候，根据avg进行排序
//        if (res == 0) {
//            System.out.println("赞数相同。。。。。");
//            res = this.comments_count < o.getComments_count() ? 1 : (this.comments_count == o.getComments_count() ? 0 : -1);
//        }
//
//        return res;
        if(this.zan_count - o.getZan_count() != 0){
            return this.zan_count - o.getZan_count()<0?1:-1;
        }else if(this.comments_count - o.getComments_count() !=0){

            return this.comments_count - o.getComments_count()<0?1:-1;
        }else{
            return 0;
        }
    }

    //写出，作为下次的输入
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.zan_count);
        dataOutput.writeInt(this.comments_count);
    }

    //读入
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.zan_count = dataInput.readInt();
        this.comments_count = dataInput.readInt();

    }

    //需要重写
    @Override
    public int hashCode() {
        return Integer.MAX_VALUE - this.zan_count;
    }

    @Override
    public String toString() {
        return this.zan_count + "," + this.comments_count;
    }

    public int getZan_count() {
        return zan_count;
    }

    public int getComments_count() {
        return comments_count;
    }
}
