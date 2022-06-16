package com.liuweiyi.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String fileName;
    private Text outKey;
    private TableBean outValue;
    /**
     * setup 为初始化方法
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitArr = line.split("\t");
        if(fileName.equals("pd")){
            outKey.set(splitArr[0]);
            outValue = TableBean.builder().amount(0).flag("pd").id("").pid(splitArr[0]).pname(splitArr[1]).build();
        }
        else{
            outKey.set(splitArr[1]);
            outValue = TableBean.builder().amount(Integer.parseInt(splitArr[2]))
                    .flag("pd").id(splitArr[0]).pid(splitArr[1]).pname("").build();
        }
        context.write(outKey,outValue);
    }
}
