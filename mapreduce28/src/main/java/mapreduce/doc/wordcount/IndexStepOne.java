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
 * @author Leo
 * @version 1.0
 * @date 2020�~7��12�� �U��5:50:09
 * @remarks TODO
 *
 */

public class IndexStepOne {
	public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		// context�]�t�n�B�z���ƾڤ������ҭn�B�z���ƾڰT��
		// ��p�ثe�o�Ӥ����O�ĴX�ӡB�����j�p�B�Ҧb���
		// map()����<hello-���W, 1>
		// inputsplit��J����:
		// �Ω�y�z�C��maptask�ҳB�z���ƾڥ��Ƚd��
		// �p�GmaptaskŪ���O���:
		// �����d�����ӥΦp�U�H���y�z:
		// �����|�B�����q�d��
		// �p�GmaptaskŪ���O�ƾڮw���ƾ�:
		// �������Ƚd�����ӥΦp�U�H���y�z:
		// �w�W.��W, �C�d��
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// �q��J�����H���������e���b�B�z���@��ƾک��ݪ����
			// InputSplit�Oabstract class
			// �]��compiler�]�����D����nŪ���ƾ���������? ���or�ƾڮw?
			InputSplit inputSplit = context.getInputSplit();
			
			// �]���{�b���D�n�����ƾ������O���A���M���ӭn�A���P�_
			// ����InputSplit�Athen ctrl+t�A�i�H��InputSplit���U����{��
			FileSplit fileSplit = (FileSplit)inputSplit;
			
			// ���o���W
			String fileName = fileSplit.getPath().getName();
			
			// maptask�|�@��Ū���@��
			String[] words = value.toString().split(" ");
			
			// �z�Lcontext�Nmaptask�B�z�᪺�ƾڿ�X��@maptask��output
			for(String w : words) {
				// �N"���-���W"��@key�A1��@value�A��X
				context.write(new Text(w + "-" + fileName), new IntWritable(1));
			}
		}
	}
	
	// Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> -> Reducer<Text, IntWritable, Text, IntWritable>
	// reducetask����maptask��output�@���L��input�A�ҥHinput�������O<Text, IntWritable>
	public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
				int count = 0;
				
				// �w��P�լۦPkey���ƾڶi��count�֥[
				for(IntWritable v : values) {
					count += v.get();
				}
				
				context.write(key, new IntWritable(count));
		}
	}
	
	// ����job
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// �bWindows Local�ݹB��A�N���γ]�m�Ѽ�
		// �q�{�u�[��core-default.xml core-site.xml
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		// �ѩ�ڭ̪�ChangePartitioner�i��|����6�ؤ��ϸ��A�ҥH�ݭn��6��reduce task�ӱ���
		job.setNumReduceTasks(3); // �ھ�ChangePartioner����^�ƶq�M�w
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/docwordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/docwordcount/output"));
		
		job.waitForCompletion(true);
	}

}
