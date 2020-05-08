//Ziming Dong
//CSE 512 Assignment 4

import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin
{
    public static class TupleMapper extends Mapper <LongWritable, Text, DoubleWritable, Text>
    {
        private Text outputValue = new Text();
        private DoubleWritable outputKey = new DoubleWritable();
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
            String comma=",";
            String[] tuple=value.toString().split(comma);
            Text id= new Text(tuple[1]);
            outputKey.set(Double.parseDouble(String.valueOf(id)));
            outputValue.set(String.valueOf(value));
            context.write(outputKey,outputValue);

        }

    }
    public static class TupleReducer extends Reducer<DoubleWritable, Text, Object, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Text outputText = new Text();
            List<String> records= new ArrayList<String>();
            List<String> table2= new ArrayList<String>();
            List<String> table3= new ArrayList<String>();
            StringBuilder outputStringBuilder = new StringBuilder();
            String comma=",";
            for (Text value : values){
                records.add(value.toString());
                }
            System.out.println("records");
            System.out.println(records);
            if (records.size()<2){
                return;
            }

            String name=null;
            name=records.get(0).split(",")[0];
            for (String t : records){
                if (name.equals(t.split(",")[0])){
                    table2.add(t);
                }
                else{
                    table3.add(t);
                }
            }
            if (table2.size()==0){
                return;
            }
            if (table3.size()==0){
                return;
            }

            for (String t2 :table2){
                for (String t3:table3){
                    outputStringBuilder.append(t2).append(comma).append(" ").append(t3).append("\n");

                }
            }

            outputText.set(outputStringBuilder.toString().trim());
            context.write(null, outputText);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration con = new Configuration();
        // Job j=new Job(con,"equijoin");
        Job j = Job.getInstance(con, "equijoin");
        j.setJarByClass(equijoin.class);
        j.setMapperClass(TupleMapper.class);
        j.setReducerClass(TupleReducer.class);
        j.setMapOutputKeyClass(DoubleWritable.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Object.class);
        j.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(j,new Path(args[1]));
        // FileOutputFormat.setOutputPath(j,new Path(args[2]));
        FileInputFormat.addInputPath(j,new Path(args[0]));
        FileOutputFormat.setOutputPath(j,new Path(args[1]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}
