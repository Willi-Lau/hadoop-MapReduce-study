package com.liuweiyi.mapreduce.combine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 此例子实现基础的mapReduce
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {

        //1 获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2 设置jar路径
        job.setJarByClass(WordCountDriver.class);
        //3 关联mapper 与reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4 设置map输出的key value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置 combiner 这里的combiner和 reducer一样所以可以采用 reducer作为combiner
        //job.setCombinerClass(WordCountCombiner.class);
        job.setCombinerClass(WordCountReducer.class);
        //5 设置最终输出key value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6 设置输入路径 输出路径
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\Administrator\\Desktop\\hadoopTest\\example\\hello.txt"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\Administrator\\Desktop\\hadoopTest\\example\\result"));
        //7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);



    }
}
