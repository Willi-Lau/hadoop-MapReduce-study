package com.liuweiyi.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN : map阶段输入的key的类型 LongWritable
 * VALUEIN : map阶段输入的value类型 ： Text
 * KEYOUT : map阶段输出的key类型： Text
 * VALUEOUT : map阶段输出的value类型 IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //放到这里原因是 map会来会调用 会增加JVM heap压力
    private Text keyOut = new Text();
    private IntWritable intWritable = new IntWritable();

    /**
     * 需要自己重写
     *
     * @param key     每一行的key
     * @param value   每一行的value
     * @param context 承载上下文核心数据的地方
     * @throws IOException
     * @throws InterruptedException 在map阶段不需要对数据进行统计 只需要进行切割即可 所以 valueOut 为 1 不需要具体指定
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] words = line.split(" ");
        //循环写出 这里的作用主要是切割字符串 拿到text 不需要聚合操作
        for (String word : words) {
            keyOut.set(word);
            intWritable.set(1);
            context.write(keyOut, intWritable);
        }

    }
}
