package com.liuweiyi.mapreduce.outputformat;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 此模块需要根据不同的输入网址将结果输出到不同的log中，这里根据流获取分配
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOutputStream;
    private FSDataOutputStream otherOutputStream;

    //流创建部分
    public LogRecordWriter(TaskAttemptContext job) {
        //创建两条流
        try {
            //和 job建立连接
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            //创建流
            atguiguOutputStream = fileSystem.create(new Path("D:\\Hadoop\\atuigu.log"));
            otherOutputStream = fileSystem.create(new Path("D:\\Hadoop\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //流具体写入流程 将包含 atguigu部分分到一个log 其他的分到别的log
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        if (log.contains("atguigu")) {
            atguiguOutputStream.writeBytes(log + "\n");
        } else {
            otherOutputStream.writeBytes(log + "\n");
        }
    }

    //关闭流操作
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOutputStream);
        IOUtils.closeStream(otherOutputStream);
    }
}
