package mapreduce.wordcount.skew;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.wordcount.JobSubmitterLinuxYarn;
import mapreduce.wordcount.WordCountCombiner;
import mapreduce.wordcount.WordCountMapper;
import mapreduce.wordcount.WordCountReducer;

public class SkewWordCount2 {
	public static class SkewWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text k = new Text();
		IntWritable v = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] wordAndCount = value.toString().split("\t");
			v.set(Integer.parseInt(wordAndCount[1]));
			k.set(wordAndCount[0].split("\001")[0]);
			
			context.write(k, v);
		}
	}
	
	public static class SkewWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		IntWritable v = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			
			for (IntWritable value : values) {
				count += value.get();
			}
			
			v.set(count);
			context.write(key, v);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 拿到一個Job對象，來封裝參數
		Job job = Job.getInstance(conf);

		// 透過當前類別，告訴job要提交的jar包的所在路徑
		job.setJarByClass(JobSubmitterLinuxYarn.class);

		job.setMapperClass(SkewWordCountMapper.class);
		job.setReducerClass(SkewWordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 設置map task端的局部聚合邏輯類
		job.setCombinerClass(SkewWordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/mapreduce_test/wordcount/skew_out"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/mapreduce_test/wordcount/skew_out2"));

		job.setNumReduceTasks(3);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
