package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 用於提交mapreduce job的客戶端程序到Linux中的Yarn集群上運行(把打包好的jar包提交給Yarn)
 * 也就是說，在Eclipse執行JobSubmiiter的main()，通過對Job(包含map task和reduce task)的配置，把jar包交給Yarn集群
 * Yarn集群就會拿jar包裡的Mapper和Reducer來運行(而不是寫在Eclipse中的Mapper和Reducer)
 * 
 * 從Eclipse上執行main()，以將jar包提交給Yarn(背後會使用java -cp指令)，Yarn會自動調用所使用到的依賴包，這和從Linux上執行job客戶端程序不同
 * 若要在Linux上運行JobSubmitter，而使用java -cp來運行的話，必須一個一個寫該程序所要使用到的依賴包，這會很麻煩
 * 所以要使用Hadoop jar ..jar JobSubmitter來執行(詳細說明在JobSubmitterLinuxYarn)
 * 
 * 功能：
 * 1. 封裝本次job運行時所需要的必要參數
 * 2. 跟yarn進行交互，將mapreduce程序成功的啟動、運行
 * @author Leo
 * @version 1.0
 * @date 2019年9月29日 上午1:32:14
 * @remarks TODO
 *
 */

public class JobSubmitterWindowsYarn {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		// 在代碼中設置JVM系統參數，用於給job對象以獲取訪問HDFS的用戶身分
		System.setProperty("HADOOP_USER_NAME", "root");
		
		// 1. 設置job運行時要訪問的默認文件系統，不設置這個的話，hadoop會去找core-default.xml，該文件裡配置的fs.defaultFS默認為file://
		conf.set("fs.defaultFS", "hdfs://hdp-01:9000");
		// 2. 設置job提交到哪去運行(以甚麼模式運行：local或yarn)，這裡把job提交到yarn集群中運行
		conf.set("mapreduce.framework.name", "yarn");
		// 3. 設置resourcemanager
		conf.set("yarn.resourcemanager.hostname", "hdp-01");
		// 4. 如果要從windows系統上運行這個job提交客戶端程序到Linux上的yarn集群去執行，則需要設置這個跨平台提交的參數為true
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		Job job = Job.getInstance(conf);

		// 1. 封裝參數：jar包所在的位置
		job.setJar("D:/Apache Ecosystem/myjars/wordcount.jar");
//		job.setJarByClass(JobSubmitter.class); // 取得該類(可以自動獲取該類所在路徑，也就是可以透過該類取得當前的Jar所在路徑，藉此取得Jar包)，setJarByClass()可以動態取得類所在位置
		
		// 2. 封裝參數：本次job所要調用的Mapper實現類、Reduce實現類
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 3. 封裝參數：本次job的Mapper實現類、Reducer實現類產生的結果數據的key、value類型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		Path output_maven = new Path("hdfs://hdp-01:9000/wordcount/output_maven");
//		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
//		if(fs.exists(output_maven)) {
//			fs.delete(output_maven, true);
//		}
		
		// 4. 封裝參數：本次job要處理的輸入數據及所在路徑、最終結果的輸出路徑
		// 只要在hadoop下的configuration明確配置文件系統如: hdfs://hdp-01:9000/
		// HDFS、MapReduce、Yarn都會使用這個使用文件系統，之後指定Path只需再寫以下的路徑即可
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output_maven")); // 輸出路徑必須不存在
		
		// 5. 封裝參數：想要啟動的reduce task的數量
		job.setNumReduceTasks(2); // 默認一個
		
		// 6. 提交job給yarn
		// job.waitForCompletion()：等待mapreduce完成，會一直跟resource manager保持聯繫，且
		// resource manager會向客戶端把一些進度訊息進行反饋
		// 參數：是否要把resource manager返回的參數印出來
		// 返回值：這次mapreduce程序的運行結果是否成功
		boolean result = job.waitForCompletion(true);
		
		// 可於啟動腳本獲取一個自定的返回值：以判斷mapreduce是否運行成功，因為main方法不返回值的關係
		System.exit(result? 0: -1);
	}
}












