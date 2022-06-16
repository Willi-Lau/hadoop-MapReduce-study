package com.liuweiyi.mapreduce.writable;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * 此例子实现对象序列化反序列化
 */
@NoArgsConstructor
@Data
public class FlowBean implements Writable {
    //上行流量
    private long upFlow;
    //下行流量
    private long downFlow;
    //总流量
    private long sumFlow;

    public void setSumFlow(){
        this.sumFlow = this.upFlow + this.downFlow;
    }
    //序列化 注意顺序和反序列化一致
    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeLong(upFlow);
         dataOutput.writeLong(downFlow);
         dataOutput.writeLong(sumFlow);
    }
    //反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return  upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }
}
