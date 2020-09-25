package mapreduce.page;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitter {
	
	public static void main(String[] args) throws Exception{
		// �bWindows Local�ݹB��A�N���γ]�m�Ѽ�
		// 1. �i�H�q�L�[��classpath�U��*-site.xml���ѪR�Ѽ�
		// ���[conf.addResource()�Ahdfs�u�|�۰ʥ[��core-site.xml�Mhdfs-site.xml�A��L���{�o
		// ����L��*-site.xml�A�ݭn�z�Lcon.addResource()�~�ॿ�T�[��
		Configuration conf = new Configuration();
		conf.addResource("*-site.xml");
		
		// 2. �q�L�N�X�]�m�Ѽ�
//		conf.setInt("top.n", 3); // �]�w�ۭq�ѼƦW�M��
		
		// 3. �q�L�ݩʰt�m�������Ѽ�(�t�mtopn.properties���)�A�i�Ѧҫe�����Ҥl
		
		// conf�Q�ʸ˨�Job�̡A����N�i�H�qJob���o�����Ѽ�(�Ҧp:�qcontext���o)
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(PageTopnMapper.class);
		job.setReducerClass(PageTopnReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/flow/output"));
		
		job.waitForCompletion(true);
	}

}
