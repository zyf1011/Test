package com.huike.test00;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class combinner {
    public static class MyMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
            String str = values.toString();
            String[] splited = str.split(" ");

            for(String word:splited){
                context.write(new Text(word),new LongWritable(1L));
                System.out.println("Mapper输出<" + word + "," + 1 + ">");
            }
        }
    }

    public static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
            System.out.println("Reducer输入分组《"+key.toString()+",N(N>=1)>");

            long count = 0L;
            for (LongWritable value:values){
                count += value.get();
                //显示次数表示输入的k2,v2的键值对数量
                System.out.println("Combiner输入键值对<" + key.toString() + ","
                        + value.get() + ">");
            }
            context.write(key,new LongWritable(count));
            System.out.println("Combiner输出键值对<" + key.toString() + "," + count
                    + ">");
        }
    }

    public static class MyCombinner extends Reducer<Text,LongWritable,Text,LongWritable>{
        protected void reducer(Text key,Iterable<LongWritable> values,Reducer<Text,LongWritable,Text,LongWritable>.Context context) throws IOException, InterruptedException {
            System.out.println("Combiner输入分组<" + key.toString() + ",N(N>=1)>");

            long count = 0L;
            for(LongWritable value:values){
                count += value.get();
                System.out.println("Combiner输入分组《"+key.toString()+","+value.get()+">");
            }
            context.write(key,new LongWritable(count));
            System.out.println("Combiner输出键值对<" + key.toString() + "," + count
                    + ">");
        }
    }

    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.err.println("Usage:<in><out>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.isDirectory(output)){
            fs.delete(output,true);
        }


        Job job = Job.getInstance(conf);

        job.setJarByClass(combinner.class);

        job.waitForCompletion(true)
    }
}
