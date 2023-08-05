package com.axing._11JoinDemo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableBean implements Writable {

    private String id; //订单id
    private String pId; //商品id
    private int amount; //商品数量
    private String pName;
    private String flag;// 标记什么表


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id); // writeUTF 写String
        out.writeUTF(pId);
        out.write(amount);
        out.writeUTF(pName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pId = in.readUTF();
        amount = in.readInt();
        pName = in.readUTF();
        flag = in.readUTF();
    }

    @Override
    public String toString() {
        return id + '\t' + pName + '\t' + amount + '\t';

    }
}
