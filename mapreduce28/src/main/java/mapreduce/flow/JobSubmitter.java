package mapreduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitter {
	
	public static void main(String[] args) throws Exception{
		// �bWindows Local�ݹB��A�N���γ]�m�Ѽ�
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		// �]�m�Ѽ�: maptask�b���ƾڤ��ϮɡA�έ��Ӥ����޿����A�p�G�����w�A�L�|���q�{��HashPartitioner
		job.setPartitionerClass(ChangePartitioner.class);
		// �ѩ�ڭ̪�ChangePartitioner�i��|������6�ؤ��ϸ��A�ҥH�ݭn��6��reduce task�ӱ���
		job.setNumReduceTasks(6); // �ھ�ChangePartioner����^�ƶq�M�w
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/flow/output"));
		
		job.waitForCompletion(true);
	}

}
