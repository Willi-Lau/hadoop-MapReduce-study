package com.liuweiyi.mapreduce.outputformat;

import com.liuweiyi.mapreduce.partitionAndWritableComparable.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogDriver {
    public static void main(String[] args) throws Exception{
        //获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar
        job.setJarByClass(LogDriver.class);
        //关联mapper reducer
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);
        //设置mapper输出的key value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输出的key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义的outputformat
        job.setOutputFormatClass(LogOutputFormat.class);

        //设置数据输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(""));
        //虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job,new Path(""));
        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
