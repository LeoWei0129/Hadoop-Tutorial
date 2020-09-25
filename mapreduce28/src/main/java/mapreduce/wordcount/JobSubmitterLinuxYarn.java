package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * �p�G�n�bHadoop���s���Y�x�����W(Linux)�����Ұʳo��job����Ȥ�ݵ{�Ǫ���
 * conf�̪����N���ݭn���wfs.defaultFS�Bmapreduce.framework.name
 * 
 * �]���b���s�����W��hadoop jar xx.jar mapreduce.wordcount.JobSubmitterLinux�R�O�ӱҰʫȤ��min��k�ɡA
 * hadoop jar�o�өR�O�|�N�Ҧb�����W��hadoop�w�˥ؿ�����jar�]�M�t�m���[�J��B��ɪ�classpath��
 * (�Ұʵ{�ǮɡAjvm�|�hclasspath���d��|�Ψ쪺class)
 * 
 * ����A�ڭ̪��Ȥ��main��k����new Configuration()�y�y�N�|�[��classpath�����t�m���A�۵M�N���F
 * fs.defaultFS�Bmapreduce.framework.name�Myarn.resourcemanager.hostname�o�ǰѼưt�m
 * 
 * @author Leo
 * @version 1.0
 * @date 2019�~10��6�� �U��5:35:22
 * @remarks TODO
 *
 */

public class JobSubmitterLinuxYarn {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		// �S���w�q�{�����t��
		// �S���wmapreduce�������B��
		
		// ����@��Job��H�A�ӫʸ˰Ѽ�
		Job job = Job.getInstance(conf);
		
		// �z�L��e���O�A�i�Djob�n���檺jar�]���Ҧb���|
		job.setJarByClass(JobSubmitterLinuxYarn.class); 
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));
		
		job.setNumReduceTasks(3);
		
		boolean res = job.waitForCompletion(true);
		System.exit(res? 0: 1);
	
	}
}
