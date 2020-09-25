package mapreduce.doc.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapreduce.flow.ChangePartitioner;
import mapreduce.flow.FlowBean;
import mapreduce.flow.FlowSumMapper;
import mapreduce.flow.FlowSumReducer;
import mapreduce.flow.JobSubmitter;

/**
 * 
 * 從輸入切片信息中獲取當前正在處理的一行數據所屬的文件
 * @author Leo
 * @version 1.0
 * @date 2020年7月12日 下午5:50:09
 * @remarks TODO
 *
 */

public class IndexStepOne {
	public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		// context包含要處理的數據切片中所要處理的數據訊息
		// 比如目前這個切片是第幾個、切片大小、所在文件等
		// map()產生<hello-文件名, 1>
		// inputsplit輸入切片:
		// 用於描述每個maptask所處理的數據任務範圍
		// 如果maptask讀的是文件:
		// 劃分範圍應該用如下信息描述:
		// 文件路徑、偏移量範圍
		// 如果maptask讀的是數據庫的數據:
		// 劃分任務範圍應該用如下信息描述:
		// 庫名.表名, 列範圍
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 從輸入切片信息中獲取當前正在處理的一行數據所屬的文件
			// InputSplit是abstract class
			// 因為compiler也不知道之後要讀的數據類型為何? 文件or數據庫?
			InputSplit inputSplit = context.getInputSplit();
			
			// 因為現在知道要取的數據類型是文件，不然應該要再做判斷
			// 雙擊InputSplit，then ctrl+t，可以看InputSplit底下的實現類
			FileSplit fileSplit = (FileSplit)inputSplit;
			
			// 取得文件名
			String fileName = fileSplit.getPath().getName();
			
			// maptask會一次讀取一行
			String[] words = value.toString().split(" ");
			
			// 透過context將maptask處理後的數據輸出當作maptask的output
			for(String w : words) {
				// 將"單詞-文件名"當作key，1當作value，輸出
				context.write(new Text(w + "-" + fileName), new IntWritable(1));
			}
		}
	}
	
	// Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> -> Reducer<Text, IntWritable, Text, IntWritable>
	// reducetask接收maptask的output作為他的input，所以input的類型是<Text, IntWritable>
	public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
				int count = 0;
				
				// 針對同組相同key的數據進行count累加
				for(IntWritable v : values) {
					count += v.get();
				}
				
				context.write(key, new IntWritable(count));
		}
	}
	
	// 提交job
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 在Windows Local端運行，就不用設置參數
		// 默認只加載core-default.xml core-site.xml
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		// 由於我們的ChangePartitioner可能會產生6種分區號，所以需要有6個reduce task來接收
		job.setNumReduceTasks(3); // 根據ChangePartioner的返回數量決定
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/docwordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/docwordcount/output"));
		
		job.waitForCompletion(true);
	}

}
