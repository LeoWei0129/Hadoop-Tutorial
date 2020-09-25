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
 * 如果要在Hadoop集群的某台機器上(Linux)直接啟動這個job提交客戶端程序的話
 * conf裡的面就不需要指定fs.defaultFS、mapreduce.framework.name
 * 
 * 因為在集群機器上用hadoop jar xx.jar mapreduce.wordcount.JobSubmitterLinux命令來啟動客戶端min方法時，
 * hadoop jar這個命令會將所在機器上的hadoop安裝目錄中的jar包和配置文件加入到運行時的classpath中
 * (啟動程序時，jvm會去classpath中查找會用到的class)
 * 
 * 那麼，我們的客戶端main方法中的new Configuration()語句就會加載classpath中的配置文件，自然就有了
 * fs.defaultFS、mapreduce.framework.name和yarn.resourcemanager.hostname這些參數配置
 * 
 * @author Leo
 * @version 1.0
 * @date 2019年10月6日 下午5:35:22
 * @remarks TODO
 *
 */

public class JobSubmitterLinuxYarn {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		// 沒指定默認的文件系統
		// 沒指定mapreduce提交到哪運行
		
		// 拿到一個Job對象，來封裝參數
		Job job = Job.getInstance(conf);
		
		// 透過當前類別，告訴job要提交的jar包的所在路徑
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
