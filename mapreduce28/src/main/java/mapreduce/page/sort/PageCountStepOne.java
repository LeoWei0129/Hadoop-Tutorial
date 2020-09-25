package mapreduce.page.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.page.JobSubmitter;
import mapreduce.page.PageTopnMapper;
import mapreduce.page.PageTopnReducer;

/**
 * 
 * 不透過TreeMap來排序key，因為TreeMap會被存在記憶體，若資料量太大，記憶體無法負荷
 * 這個類的Mapper和Reducer只是將頁面page做一個數量統計，尚未根據數量排序
 * 所以再寫一個類，它包含了另一組Mapper和Reducer，根據訪問次數進行排序並輸出
 * @author Leo
 * @version 1.0
 * @date 2019年10月14日 下午11:21:59
 * @remarks TODO
 *
 */

public class PageCountStepOne {
	// 透過內部類實現Mapper和Reducer，比較簡潔
	public static class PageCountSortStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split(" ");
			context.write(new Text(split[1]), new IntWritable(1));

		}
	}

	public static class PageCountSortStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;

			for (IntWritable value : values) {
				count += value.get();
			}

			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 在Windows Local端運行，就不用設置參數
		// 1. 可以通過加載classpath下的*-site.xml文件解析參數
		// 不加conf.addResource()，hdfs只會自動加載core-site.xml和hdfs-site.xml，其他不認得
		// 對於其他的*-site.xml，需要透過con.addResource()才能正確加載
		Configuration conf = new Configuration();
		conf.addResource("*-site.xml");

		// 2. 通過代碼設置參數
		// conf.setInt("top.n", 3); // 設定自訂參數名和值

		// 3. 通過屬性配置文件獲取參數(配置topn.properties文件)，可參考前面的例子

		// conf被封裝到Job裡，之後就可以從Job取得相關參數(例如:從context取得)
		Job job = Job.getInstance(conf);

		job.setJarByClass(JobSubmitter.class);

		job.setMapperClass(PageCountSortStepOneMapper.class);
		job.setReducerClass(PageCountSortStepOneReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/sort/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/sort/output"));

		job.setNumReduceTasks(3);
		
		job.waitForCompletion(true);
	}
}
