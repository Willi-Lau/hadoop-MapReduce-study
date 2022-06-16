package com.liuweiyi.mapreduce.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN : reduce阶段输入的key的类型 Text (map的输出)
 * VALUEIN : reduce阶段输入的value类型 ： IntWritable (map的输出)
 * KEYOUT : reduce阶段输出的key类型： Text
 * VALUEOUT : reduce阶段输出的value类型 IntWritable
 * <p>
 * map阶段传进来的数据：
 * KEYIN    VALUEIN
 * liuweiyi  1
 * liuweiyi  1
 * dsp       1
 * dsp       1
 * kafka     1
 * 输出的数据：
 * KEYOUT    VALUEOUT
 * liuweiyi  2
 * dsp       2
 * kafka     1
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * @param key e.g. liuweiyi
     * @param values 这里是一个类似集合的值 e.g. (1,1)
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("------------------------"+key + " " + values);
        int sum = 0;
        // key liuweiyi values (1,1)  ？？？这里是怎么变化过来的 看源码没找到
        for (IntWritable value : values){
            sum += value.get();
        }
        //写出
        context.write(key , new IntWritable(sum));
    }
}
