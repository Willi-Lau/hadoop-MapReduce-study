package com.liuweiyi.mapreduce.partitionAndWritableComparable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

    public static void main(String[] args) throws Exception{
        //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar
        job.setJarByClass(FlowDriver.class);
        //关联mapper reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //设置mapper输出的key value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终输出的key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置分区器
        job.setPartitionerClass(ProvincePartition.class);
        //设置对应Task数量 需要和分区数对应
        job.setNumReduceTasks(5);

        //设置数据输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(""));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\Administrator\\Desktop\\hadoopTest\\example\\result"));
        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
