package com.huike.test00;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class fin1 {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(fin1.class);

        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputpath = new Path("fff");
        Path output = new Path("oo");
        FileSystem fs  =FileSystem.get(conf);
        if(fs.isDirectory(output)){
            fs.delete(output,true);
        }
        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,output);

        boolean w = job.waitForCompletion(true);
        System.exit(w?0:1);
    }

    private static class mapper extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
            String line = values.toString();
            String[] token = line.split(":");
            String person = token[0];
            String[] fans = token[1].split(",");
            /**
             * 写出的数据格式：
             * AB ---> A
             * AB ---> B
             * 如果AB是互粉对，那么必然 AB 所对应的的 values组合会有两个值，也就是必定会有 A 和 B
             */
            for (String fan:fans){
                context.write(new Text(combinnerStr(person,fan)),new Text(person));
            }
        }

        private String combinnerStr(String a,String b){
            if(a.compareTo(b)>0){
                return b+"-"+a;
            }else {
                return a+"-"+b;
            }
        }
    }

    //NullWritable是Writable的一个特殊类，实现方法为空实现，不从数据流中读数据，也不写入数据，只充当占位符，
    // 如在MapReduce中，如果你不需要使用键或值，你就可以将键或值声明为NullWritable,NullWritable是一个不可变
    // 的单实例类型。
    private static class reducer extends Reducer<Text,Text,Text,NullWritable>{
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text t:values){
                sum++;
            }
            if(sum==2){
                context.write(key,NullWritable.get());
            }
        }
    }

}
