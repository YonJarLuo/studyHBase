首先.需搭建好 Hadoop集群环境
然后.搭建zookeeper集群
最后.搭建HBase集群

启动HBase服务顺序：先启动Hadoop（hdfs,yarn）,接着启动ZK,最后启动HBase

命令:
sh start-all.sh;
sh zkServer.sh start;
sh start-hbase.sh;

此项目为操作HBase的demo
很多健壮性等细节部分此处省略
