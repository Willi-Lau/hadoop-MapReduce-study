package com.liuweiyi.mapreduce.writeableCompareable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 根据 flowbean排序
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean outKey = new FlowBean();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        outValue.set(split[0]);
        outKey.setUpFlow(Long.getLong(split[1]));
        outKey.setUpFlow(Long.getLong(split[2]));
        outKey.setSumFlow();
        context.write(outKey, outValue);
    }
}
