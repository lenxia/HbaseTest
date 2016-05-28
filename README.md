+ 本版本采用hbase0.98.x

#1、对hbase性能测试（通过单线程和多线程的形式进行读写）
    --demo01中是对hbase的操作

    --demo02中是对hbase进行性能测试
    
    --demo03是对hbase二次排序

#2、对hbase进行二次排序
    --须知：MapReduce在Map端排序时，都只按key排序
    --需求：
          现在我在HBase里面，存储了10万条文献的评论统计记录，每个文献的评论数目是2~20，每个文献评论统计记录在HBase的存储示例如下：
          即表 “pb_stat_comments_count”的记录
          [rowkey, column family, column qualifer, value] =
          [literature row key, 'info', 'count', 12]
          [literature row key, 'info', 'avgts', 1400815843854]
          count:是每个文献的记录
          avgts：是所有文献的平均评价时间戳
          排序要求：将文献按照评价次数逆序排，次数大的靠前，次数相同的按照平均评价时间排序。

    --思路：
    1、自定义组合key，通过我们自定义的规则对map端key的输出进行排序
    2、自定义partitioner（）
    3、自定义分组
    
#3、知识要点
    --mr 过程
    --mr shuffle
