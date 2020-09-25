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
 * �b�}�o���ն��q�A�����bwindows���a�ݹB��job�A�t�׷|�֫ܦh
 * �B�����bEclipse����M�����A�|����e��
 * �o��k�ݭn�t�m%HADOOP_HOME%/bin�������ܫG
 * @author Leo
 * @version 1.0
 * @date 2019�~10��6�� �U��6:00:52
 * @remarks TODO
 *
 */

public class JobSubmitterWindowsLocal {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// �U����ӰѼƬ�core-default.xml�Mmapred-default�q�{���A�i�H���μg�X��
		// ���w�q�{�����t��
//		conf.set("fs.defaultFS", "file:///");
		
		// ���wmapreduce�������B��
//		conf.set("mapreduce.framework.name", "local");
		

		// ����@��Job��H�A�ӫʸ˰Ѽ�
		Job job = Job.getInstance(conf);

		// �z�L��e���O�A�i�Djob�n���檺jar�]���Ҧb���|
		job.setJarByClass(JobSubmitterLinuxYarn.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// �]�mmap task�ݪ������E�X�޿���
		job.setCombinerClass(WordCountCombiner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/local/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/local/wordcount/output"));

		job.setNumReduceTasks(3);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}
