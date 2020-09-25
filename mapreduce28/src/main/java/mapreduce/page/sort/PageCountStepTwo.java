package mapreduce.page.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.page.JobSubmitter;
import mapreduce.page.sort.PageCountStepOne.PageCountSortStepOneMapper;
import mapreduce.page.sort.PageCountStepOne.PageCountSortStepOneReducer;

public class PageCountStepTwo {
	public static class PageCountSortStepTwoMapper extends Mapper<LongWritable, Text, PageCount, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// �W�@��reducer��X�����C��榡���Gpage\t����
			String[] split = value.toString().split("\t");
			
			PageCount pageCount = new PageCount();
			pageCount.set(split[0], Integer.parseInt(split[1]));
			
			// NullWritable.get()��^�@��Hadoop�{�o��null����
			context.write(pageCount, NullWritable.get());
		}
	}
	
	public static class PageCountSortStepTwoReducer extends Reducer<PageCount, NullWritable, PageCount, NullWritable> {
		@Override
		protected void reduce(PageCount key, Iterable<NullWritable> arg1,
				Reducer<PageCount, NullWritable, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// reduce()�����쪺�@��key-value���ǬO�w�g���Ӧ۩w�q���j�p�ƧǦn��(�g�bPageCount��comparable��)
			// �`���Ƥj��page�|���Qreduce()������
			// �ݭn��{toString()�A�~��Nkey�qReducer�ǿ�X�h(Mapper�����w)
			// ���X�O�Ӫ���ɡA�g��~����󪺳o��key�ȷ|�O����ե�toString()����^��
			// �ҥH�ڭ̥i�H�۩w�q�@��toString()�A�M�w�n��^�����G
			context.write(key, NullWritable.get());
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

		job.setMapperClass(PageCountSortStepTwoMapper.class);
		job.setReducerClass(PageCountSortStepTwoReducer.class);

		job.setMapOutputKeyClass(PageCount.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(PageCount.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/sort/output"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/sort/sortoutput"));

		job.setNumReduceTasks(1); // �o�ˤ~��b�@��reduce task���p��������G
		
		job.waitForCompletion(true);
	}
}
