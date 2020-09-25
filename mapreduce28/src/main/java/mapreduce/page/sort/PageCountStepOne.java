package mapreduce.page.sort;

import java.io.IOException;

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

import mapreduce.page.JobSubmitter;
import mapreduce.page.PageTopnMapper;
import mapreduce.page.PageTopnReducer;

/**
 * 
 * ���z�LTreeMap�ӱƧ�key�A�]��TreeMap�|�Q�s�b�O����A�Y��ƶq�Ӥj�A�O����L�k�t��
 * �o������Mapper�MReducer�u�O�N����page���@�Ӽƶq�έp�A�|���ھڼƶq�Ƨ�
 * �ҥH�A�g�@�����A���]�t�F�t�@��Mapper�MReducer�A�ھڳX�ݦ��ƶi��ƧǨÿ�X
 * @author Leo
 * @version 1.0
 * @date 2019�~10��14�� �U��11:21:59
 * @remarks TODO
 *
 */

public class PageCountStepOne {
	// �z�L��������{Mapper�MReducer�A���²��
	public static class PageCountSortStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));

		}
	}

	public static class PageCountSortStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;

			for (IntWritable value : values) {
				count += value.get();
			}

			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// �bWindows Local�ݹB��A�N���γ]�m�Ѽ�
		// 1. �i�H�q�L�[��classpath�U��*-site.xml���ѪR�Ѽ�
		// ���[conf.addResource()�Ahdfs�u�|�۰ʥ[��core-site.xml�Mhdfs-site.xml�A��L���{�o
		// ����L��*-site.xml�A�ݭn�z�Lcon.addResource()�~�ॿ�T�[��
		Configuration conf = new Configuration();
		conf.addResource("*-site.xml");

		// 2. �q�L�N�X�]�m�Ѽ�
		// conf.setInt("top.n", 3); // �]�w�ۭq�ѼƦW�M��

		// 3. �q�L�ݩʰt�m�������Ѽ�(�t�mtopn.properties���)�A�i�Ѧҫe�����Ҥl

		// conf�Q�ʸ˨�Job�̡A����N�i�H�qJob���o�����Ѽ�(�Ҧp:�qcontext���o)
		Job job = Job.getInstance(conf);

		job.setJarByClass(JobSubmitter.class);

		job.setMapperClass(PageCountSortStepOneMapper.class);
		job.setReducerClass(PageCountSortStepOneReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/sort/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/sort/output"));

		job.setNumReduceTasks(3);
		
		job.waitForCompletion(true);
	}
}
