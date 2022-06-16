package com.liuweiyi.mapreduce.writeableCompareable;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * 此例子实现排序存储
 */
/**
 * WritableComparable就是内部排序需要实现的接口 并重写 compareTo方法
 *  WritableComparable 这个泛型一定要加 和谁比较就加谁 不加默认和object比较
 */
@NoArgsConstructor
@Data
public class FlowBean implements WritableComparable<FlowBean> {
    //上行流量
    private long upFlow;
    //下行流量
    private long downFlow;
    //总流量
    private long sumFlow;

    public void setSumFlow() {
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
        return upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    /**
     * 比较的方法
     *
     * @param flowBean
     * @return
     */
    public int compareTo(FlowBean flowBean) {
        //总流量倒叙排序 第一次排序
        if (this.sumFlow > flowBean.sumFlow) {
            return -1;
        } else if (this.sumFlow < flowBean.sumFlow) {
            return 1;
        } else {
            //上行流量正序排序 第二次排序
            if (this.getUpFlow() > flowBean.getUpFlow()) {
                return 1;
            } else if (this.getUpFlow() < flowBean.getUpFlow()) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
