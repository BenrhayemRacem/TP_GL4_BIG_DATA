package tn.insat.tp1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object,Text,Text,DoubleWritable> {
    private Text word = new Text();
    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        word.set(line[2]);
        double cost = Double.parseDouble(line[4]);
        context.write(word, new DoubleWritable(cost));
    }
}
