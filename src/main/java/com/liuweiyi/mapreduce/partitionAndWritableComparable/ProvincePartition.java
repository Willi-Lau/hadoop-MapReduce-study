package com.liuweiyi.mapreduce.partitionAndWritableComparable;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 分区过程发生在 mapper之后reduce之前 所以泛型需要根据mapper输出决定
 * 分区位置 0 1 2 。。。必须是连续的
 * 分区类需要写入Driver中
 */
public class ProvincePartition extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {

        String phone = text.toString();
        String prevPhone = phone.substring(0,3);
        if("136".equals(prevPhone)){
            return 0;
        }
        else if("137".equals(prevPhone)){
            return 1;
        }
        else if("138".equals(prevPhone)){
            return 2;
        }
        else if("139".equals(prevPhone)){
            return 3;
        }
        else {
            return 4;
        }
    }
}
