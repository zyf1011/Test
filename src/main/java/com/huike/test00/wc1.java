package com.huike.test00;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

import java.util.StringTokenizer;

public class wc1 extends Configured implements Tool {
    public static class map extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(values.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }

    public static class  reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value:values){
                sum+=value.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        if(strings.length<2){
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);

        }
        //System.exit(2)  非正常退出
        //3，Usage: wordcount <in> <out> 只是输出的日志 可以随便自定义 这里表示wordcount 这个任务 输入输出参数异常~
        //读取jar
        job.setJarByClass(wc1.class);

        Path in = new Path(strings[0]);// 输入路径
        Path out = new Path(strings[1]);// 输出路径
        FileSystem hdfs = out.getFileSystem(conf);
        if(hdfs.isDirectory(out)){
            hdfs.delete(out,true);
        }


        job.setMapperClass(map.class);
        job.setReducerClass(reducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new WordCount(),args);
        System.exit(res);
    }
}
