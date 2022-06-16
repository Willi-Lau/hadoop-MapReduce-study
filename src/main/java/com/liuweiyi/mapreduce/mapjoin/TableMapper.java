package com.liuweiyi.mapreduce.mapjoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class TableMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Map<String,String> cacheMap = new HashMap<String, String>(16);
    private Text text = new Text();
    /**
     * setup 为初始化方法
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //通过缓存获取到小表 pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        //获取文件系统对象
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        //通过包装流转换为reader,方便按行读取
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream,"UTF-8"));
        String line;
        while (StringUtils.isEmpty(line = bufferedReader.readLine())){
            String[] split = line.split("\t");
            cacheMap.put(split[0],split[1]);
        }
        bufferedReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String name = cacheMap.get(fields[1]);
        //输出到一行
        text.set(fields[0] + "\t" + name + "\t" + fields[2]);
        context.write(text,NullWritable.get());

    }
}
