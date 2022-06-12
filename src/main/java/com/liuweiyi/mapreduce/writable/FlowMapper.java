package com.liuweiyi.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] split = line.split("\t");
        //抓取想要的数据
        //手机号
        String phone = split[1];
        //上行和下行
        String up = split[split.length-3];
        String down = split[split.length-2];
        //封装输出数据
        outK.set(phone);
        outV.setSumFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();


    }
}
