package mapreduce.doc.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.flow.ChangePartitioner;
import mapreduce.flow.FlowBean;
import mapreduce.flow.FlowSumMapper;
import mapreduce.flow.FlowSumReducer;
import mapreduce.flow.JobSubmitter;

/**
 * 
 * 從輸入切片信息中獲取當前正在處理的一行數據所屬的文件
 * 
 * @author Leo
 * @version 1.0
 * @date 2020年7月12日 下午5:50:09
 * @remarks TODO
 *
 */

public class IndexStepTwo {
	public static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("-");
			context.write(new Text(split[0]), new Text(split[1].replaceAll("\t", "-->")));
		}
	}

	// Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> -> Reducer<Text, IntWritable, Text,
	// IntWritable>
	// reducetask接收maptask的output作為他的input，所以input的類型是<Text, IntWritable>
	public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
		// 每次拿到的是一組數據: <hello, a.txt-->4>, <hello, b.txt-->2>
		// 對這組數據進行迭代，做拼接到sb
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// StringBuffer是線程安全的，StringBuilder是非線程安全的
			// 在不涉及線程安全的場景下，StringBuiler更快
			StringBuilder sb = new StringBuilder();

			for (Text value : values) {
				sb.append(value.toString()).append("\t");
			}

			context.write(key, new Text(sb.toString()));
		}
	}

	// 提交job
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 在Windows Local端運行，就不用設置參數
		// 默認只加載core-default.xml core-site.xml
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(IndexStepTwo.class);

		job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReducer.class);

		// 由於我們的ChangePartitioner可能會產生6種分區號，所以需要有6個reduce task來接收
		job.setNumReduceTasks(3); // 根據ChangePartioner的返回數量決定

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/docwordcount/output"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/docwordcount/output2"));

		job.waitForCompletion(true);
	}

}
