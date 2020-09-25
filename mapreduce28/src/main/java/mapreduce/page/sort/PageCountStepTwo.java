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
			// 上一個reducer輸出的文件每行格式為：page\t次數
			String[] split = value.toString().split("\t");
			
			PageCount pageCount = new PageCount();
			pageCount.set(split[0], Integer.parseInt(split[1]));
			
			// NullWritable.get()返回一個Hadoop認得的null類型
			context.write(pageCount, NullWritable.get());
		}
	}
	
	public static class PageCountSortStepTwoReducer extends Reducer<PageCount, NullWritable, PageCount, NullWritable> {
		@Override
		protected void reduce(PageCount key, Iterable<NullWritable> arg1,
				Reducer<PageCount, NullWritable, PageCount, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// reduce()接收到的一組key-value順序是已經按照自定義的大小排序好的(寫在PageCount的comparable中)
			// 總次數大的page會先被reduce()接收到
			// 需要實現toString()，才能將key從Reducer傳輸出去(Mapper不限定)
			// 當輸出是該物件時，寫到外部文件的這個key值會是物件調用toString()的返回值
			// 所以我們可以自定義一個toString()，決定要返回的結果
			context.write(key, NullWritable.get());
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

		job.setMapperClass(PageCountSortStepTwoMapper.class);
		job.setReducerClass(PageCountSortStepTwoReducer.class);

		job.setMapOutputKeyClass(PageCount.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(PageCount.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/sort/output"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/sort/sortoutput"));

		job.setNumReduceTasks(1); // 這樣才能在一個reduce task中計算全局結果
		
		job.waitForCompletion(true);
	}
}
