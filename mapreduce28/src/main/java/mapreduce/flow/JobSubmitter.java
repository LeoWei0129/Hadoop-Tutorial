package mapreduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitter {
	
	public static void main(String[] args) throws Exception{
		// 在Windows Local端運行，就不用設置參數
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		// 設置參數: maptask在做數據分區時，用哪個分區邏輯類，如果不指定，他會用默認的HashPartitioner
		job.setPartitionerClass(ChangePartitioner.class);
		// 由於我們的ChangePartitioner可能會產ˋ生6種分區號，所以需要有6個reduce task來接收
		job.setNumReduceTasks(6); // 根據ChangePartioner的返回數量決定
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/flow/output"));
		
		job.waitForCompletion(true);
	}

}
