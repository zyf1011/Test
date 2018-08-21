package com.huike.test00;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class fin_friends {
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //指定hdfs相关的参数
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);
            //设置jar包所在路径
            job.setJarByClass(fin_friends.class);

            //指定mapper类和reducer类
            job.setMapperClass(mapper.class);
            job.setReducerClass(reducer.class);
            //指定maptask端的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //指定reducetask的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // 指定该mapreduce程序数据的输入和输出路径
            Path inputPath = new Path("D:\\\\bigdata\\\\commonfriends\\\\input");
            Path outputPath = new Path("D:\\bigdata\\commonfriends\\hufen_output");
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(outputPath)){
                fs.delete(outputPath,true);
            }
            FileInputFormat.setInputPaths(job,inputPath);
            FileOutputFormat.setOutputPath(job,outputPath);

            // 最后提交任务
            //Job运行是通过job.waitForCompletion(true)，true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
            boolean waitForCompletion = job.waitForCompletion(true);
            System.exit(waitForCompletion ? 0 : 1);


        }
        private static class mapper extends Mapper<LongWritable,Text,Text,Text>{
            protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
                String line = value.toString();
                String[] tokens = line.split(":");
                String person = tokens[0];
                String[] fans = tokens[1].split(",");

                /**
                 * 写出的数据格式：
                 * AB ---> A
                 * AB ---> B
                 * 如果AB是互粉对，那么必然 AB 所对应的的 values组合会有两个值，也就是必定会有 A 和 B
                 */

                for(String fen:fans){
                    context.write(new Text(combineStr(person,fen)),new Text(person));
                }
            }


            /**
             * 此方法的作用就是把 AB 和 BA 的组合 都编程 AB
             */
            private String combineStr(String a,String b){
                if(a.compareTo(b)>0){
                    return b+"-"+a;
                }else {
                    return a+"-"+b;
                }
            }

        }
        private static class reducer extends Reducer<Text,Text,Text,NullWritable>{
            @Override
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                for(Text t:values){
                    sum++;
                }
                if (sum == 2) {
                    context.write(key, NullWritable.get());
                }

            }
        }
    }

