package com.liuweiyi.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<Text, NullWritable,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //防止有相同数据而丢数据 这样虽然values都是NullWritable但是有几个数据 数组就会遍历几次
        for (NullWritable nullWritable : values){
            context.write(key,NullWritable.get());
        }
    }
}
