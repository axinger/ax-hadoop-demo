package com.axing._11JoinDemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private String fileName;

    private final Text outK = new Text();
    private final TableBean outV = new TableBean();

    // 初始化方法
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        // 2个文件
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();


    }

    // 每一行, 也可以获取文件,效率不高
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        //1.获取一行
        String line = value.toString();

        //2.判断文件
        if (fileName.contains("order")) {
            String[] split = line.split("\t");

            //封装KV
            outK.set(split[1]);

            outV.setId(split[0]);
            outV.setPId(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPName(""); // 不能漏值
            outV.setFlag("order");

        } else {
            String[] split = line.split("\t");

            //封装KV

            outK.set(split[0]);

            outV.setId("");
            outV.setPId(split[0]);
            outV.setAmount(0);
            outV.setPName(split[1]); // 不能漏值
            outV.setFlag("pd");

        }

        context.write(outK, outV);
    }
}
