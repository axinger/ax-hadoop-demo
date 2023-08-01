package com.axing.writable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private final FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

//        FlowBean outV = StreamSupport.stream(values.spliterator(), false).collect(outV, FlowBean::accumulate, FlowBean::combine);

        long up = 0;
        long down = 0;
        for (FlowBean bean : values) {
            up += bean.getUpFlow();
            down += bean.getDownFlow();
        }

        //封装
        outV.setUpFlow(up);
        outV.setDownFlow(down);
        outV.setSumFlow();
        //写出
        context.write(key, outV);
    }
}
