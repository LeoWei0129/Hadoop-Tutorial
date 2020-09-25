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
 * �q��J�����H���������e���b�B�z���@��ƾک��ݪ����
 * 
 * @author Leo
 * @version 1.0
 * @date 2020�~7��12�� �U��5:50:09
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
	// reducetask����maptask��output�@���L��input�A�ҥHinput�������O<Text, IntWritable>
	public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
		// �C�����쪺�O�@�ռƾ�: <hello, a.txt-->4>, <hello, b.txt-->2>
		// ��o�ռƾڶi�歡�N�A��������sb
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// StringBuffer�O�u�{�w�����AStringBuilder�O�D�u�{�w����
			// �b���A�νu�{�w���������U�AStringBuiler���
			StringBuilder sb = new StringBuilder();

			for (Text value : values) {
				sb.append(value.toString()).append("\t");
			}

			context.write(key, new Text(sb.toString()));
		}
	}

	// ����job
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// �bWindows Local�ݹB��A�N���γ]�m�Ѽ�
		// �q�{�u�[��core-default.xml core-site.xml
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(IndexStepTwo.class);

		job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReducer.class);

		// �ѩ�ڭ̪�ChangePartitioner�i��|����6�ؤ��ϸ��A�ҥH�ݭn��6��reduce task�ӱ���
		job.setNumReduceTasks(3); // �ھ�ChangePartioner����^�ƶq�M�w

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/docwordcount/output"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/docwordcount/output2"));

		job.waitForCompletion(true);
	}

}
