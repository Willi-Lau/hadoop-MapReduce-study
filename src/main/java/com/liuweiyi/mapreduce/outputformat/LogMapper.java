package com.liuweiyi.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 如果输出的东西不存在则为 NullWritable
 *
 *
 * 输入：
 *     http://baidu.com
 *     http://google.com
 *
 * 本模块作用为通过 OutPutFormat 控制流 输出不同结果到不同的文件中
 */
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value,NullWritable.get());
    }
}
