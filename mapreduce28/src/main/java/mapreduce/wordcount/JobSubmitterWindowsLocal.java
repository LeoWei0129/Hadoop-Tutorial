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
 * 在開發測試階段，直接在windows本地端運行job，速度會快很多
 * 且直接在Eclipse執行和偵錯，會比較容易
 * 這方法需要配置%HADOOP_HOME%/bin的環境變亮
 * @author Leo
 * @version 1.0
 * @date 2019年10月6日 下午6:00:52
 * @remarks TODO
 *
 */

public class JobSubmitterWindowsLocal {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 下面兩個參數為core-default.xml和mapred-default默認的，可以不用寫出來
		// 指定默認的文件系統
//		conf.set("fs.defaultFS", "file:///");
		
		// 指定mapreduce提交到哪運行
//		conf.set("mapreduce.framework.name", "local");
		

		// 拿到一個Job對象，來封裝參數
		Job job = Job.getInstance(conf);

		// 透過當前類別，告訴job要提交的jar包的所在路徑
		job.setJarByClass(JobSubmitterLinuxYarn.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 設置map task端的局部聚合邏輯類
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
