package com.liuweiyi.mapreduce.combine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 这里combiner逻辑可以写在reduce中，使用combiner和不使用combiner在执行结果层面毫无区别，
 * 而在执行效率 shuffleIO次数上 combiner明显优于单单使用 reducer
 */
public class WordCountCombiner extends Reducer<Text, IntWritable ,Text, IntWritable> {

    private IntWritable outValue = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : values){
            sum += intWritable.get();
        }
        outValue.set(sum);
        context.write(key,outValue);
    }
}
