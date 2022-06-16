package com.liuweiyi.mapreduce.reduceJoin;

import lombok.SneakyThrows;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable>{

    private List<TableBean> tableBeanList = new ArrayList<TableBean>();
    private String tableName;

    @SneakyThrows
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        boolean isExecute = false;
        for (TableBean tableBean : values) {
            String flag = tableBean.getFlag();
            if(flag.equals("pd") && !isExecute){
                tableName = tableBean.getPname();
                isExecute = true;
            }
            else{
                //hadoop中添加对象到集合需要再copy一份 不然始终都会被最后一个覆盖掉
                TableBean tempBean = new TableBean();
                BeanUtils.copyProperties(tempBean,tableBean);
                tableBeanList.add(tempBean);
            }
        }
        for (TableBean tableBean : tableBeanList) {
            tableBean.setPname(tableName);
            context.write(tableBean,NullWritable.get());
        }
    }
}
