package com.huike.action01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class a1 extends Configured implements Tool {
    public static class Mappere extends Mapper<LongWritable,Text,Text,Text>{
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] url = line.split("\\s+");
            context.write(new Text(url[0]),new Text(url[1]));
        }

    }

    public static class Combiner extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            int count = 0;
            for(Text value:values){
                sum = sum+Double.parseDouble(value.toString());
                count++;
            }
            context.write(new Text(key),new Text(sum+"_"+count));
        }

    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            Double sum = 0.00;
            int count = 0;
            for (Text t : values) {
                String[] str = t.toString().split("-");
                sum += Double.parseDouble(str[0]);
                count += Integer.parseInt(str[1]);
            }
            double average = sum / count;
            context.write(new Text(key), new Text(String.valueOf(average)));

        }

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("wenjianlijing");
        Path output = new Path("lujing");
        FileSystem hdfs = output.getFileSystem(conf);
        if(hdfs.isDirectory(output)){
            hdfs.delete(output,true);
        }

        Job job = Job.getInstance(conf,"a1");
        job.setJarByClass(a1.class);
        job.setMapperClass(Mappere.class);
        job.setReducerClass(AverageReducer.class);
        job.setCombinerClass(Combiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
       //Job运行是通过job.waitForCompletion(true)，true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        String[] args0 = { "/test/action01/Average.txt", "/test/action01/output/" };
        int res = ToolRunner.run(new Configuration(),new Average(),args0);
        System.out.println(res);
    }
}
