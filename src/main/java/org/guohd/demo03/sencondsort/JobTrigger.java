package org.guohd.demo03.sencondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by izhonhhong on 2016/5/17.
 * 执行mr任务。
 */

public class JobTrigger {
    public static void main(String args[]) {
        //定制job
        try {
            Configuration conf = HBaseConfiguration.create();
            Job job = Job.getInstance(conf, "二次排序");
            job.setJarByClass(JobTrigger.class);
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("info"));
            //cache与block的设置
            scan.setCaching(5000); //
            scan.setCacheBlocks(false);
            /*
                map reduce
                map:将hbase一张表作为map输入
                reduce:直接以Text形式输出

             */

            TableMapReduceUtil.initTableMapperJob("t_status_001", scan, SortMapper.class, SortKeyPair.class, Text.class, job);

            job.setReducerClass(SortReduce.class);


            //自定义分区，分组，排序
            job.setPartitionerClass(NaturalKeyPartitioner.class);
            job.setGroupingComparatorClass(NaturalKeyGroupComparator.class);
            job.setSortComparatorClass(CompositeKeyComparator.class);


            //设置reduce输出格式
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置结果输出hdfs路径
            FileOutputFormat.setOutputPath(job, new Path("/output/data/"));


            long start = System.currentTimeMillis();
            boolean res = job.waitForCompletion(true);
            long end = System.currentTimeMillis();

            if (res) {
                System.out.println("job done with time:" + (end - start) / 1000 + "s");
            } else {
                throw new IOException("job execute error");
            }


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

//mapper
class SortMapper extends TableMapper<SortKeyPair, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String rowkey = Bytes.toString(value.getRow());
//        int count = Bytes.toInt(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("count")));
//        long avgs = Bytes.toInt(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("count")));
        int count = Integer.valueOf(Bytes.toInt(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("zan_count"))));
        long avgs = Long.valueOf(Bytes.toInt(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("comments_count"))));

        context.write(new SortKeyPair(count, avgs), new Text(rowkey + "," + count + "," + avgs));
    }
}

//reducer
class SortReduce extends Reducer<SortMapper, Text, Text, Text> {

    @Override
    protected void reduce(SortMapper key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            context.write(null, iterator.next());
        }
    }
}

