package sarun;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

public class Neighbor {

  public static class Mapper1 extends Mapper<Object,Text,IntWritable,Message>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] values = value.toString().split("\t");
      int from = Integer.parseInt(values[0]);
      int to = Integer.parseInt(values[1]);
      context.write(new IntWritable(from), new Message(to, true));
      context.write(new IntWritable(to), new Message(from, false));
    }
  }

  public static class Reducer1 extends Reducer<IntWritable,Message,IntWritable,Message> {
    public void reduce(IntWritable key, Iterable<Message> messages, Context context) throws IOException, InterruptedException {
      
      List<Integer> fromToList = new ArrayList<Integer>();
      List<Integer> toFromList = new ArrayList<Integer>();

      for (Message message: messages) {
        if (message.isFromTo) {
          fromToList.add(message.value);
          context.write(key, message);
        } else {
          toFromList.add(message.value);
        }
      }

      for (Integer toFrom: toFromList) {
        for (Integer fromTo: fromToList) {
          context.write(new IntWritable(toFrom), new Message(fromTo.intValue(), true));
        }
      }
    }
  }

  public static class Mapper2 extends Mapper<Object,Text,Text,Text>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] values = value.toString().split("\t");
      if (values.length >= 2) {
        context.write(new Text(values[0]), new Text(values[1]));
      }
    }
  }

  public static class Reducer2 extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Set<String> neighbors = new HashSet<String>();
      for (Text value: values) {
        neighbors.add(value.toString());
      }
      context.write(key, new Text(neighbors.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1);
    job1.setJobName("Job 1");
    job1.setJarByClass(Neighbor.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(Message.class);

    Path tempOutput = new Path("temp");
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, tempOutput);
    tempOutput.getFileSystem(conf1).delete(tempOutput, true);
    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2);
    job2.setJobName("Job 2");
    job2.setJarByClass(Neighbor.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    Path outputPath = new Path("out");
    FileInputFormat.addInputPath(job2, tempOutput);
    FileOutputFormat.setOutputPath(job2, outputPath);
    outputPath.getFileSystem(conf2).delete(outputPath, true);
    System.exit(job2.waitForCompletion(true) ? 0 : 1);

    // hadoop com.sun.tools.javac.Main sarun/*.java && jar cf wc.jar sarun/*.class && hadoop jar wc.jar sarun.Neighbor test.txt && cat out/*
  }
}