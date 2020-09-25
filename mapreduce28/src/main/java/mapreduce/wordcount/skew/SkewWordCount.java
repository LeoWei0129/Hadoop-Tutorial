package mapreduce.wordcount.skew;

import java.io.IOException;
import java.util.Random;

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

import mapreduce.wordcount.JobSubmitterLinuxYarn;
import mapreduce.wordcount.WordCountCombiner;
import mapreduce.wordcount.WordCountMapper;
import mapreduce.wordcount.WordCountReducer;

public class SkewWordCount {
	public static class SkewWordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		Random random = new Random();
		Text k = new Text();
		IntWritable v = new IntWritable(1);
		int num = 0;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 * map task啟動後，會優先執行這setup()，可從這裡的context取得reduce task的數量
		 * 不用在每次調用map()時，每次都從context去取，這裡取的話只要一次就好
		 * \001代表crtl+A
		 */
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			num = context.getNumReduceTasks();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			
			for(String w : words) {
				// \001是不可見字符，不可打印字符，一般的輸入文字無法打出來，所以可以用作split切割的依據
				k.set(w + "\001" + random.nextInt(num));
				context.write(k, v);
			}
		}
	}
	
	public static class SkewWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		IntWritable v = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			
			for (IntWritable value : values) {
				count += value.get();
			}
			
			v.set(count);
			context.write(key, v);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 拿到一個Job對象，來封裝參數
		Job job = Job.getInstance(conf);

		// 透過當前類別，告訴job要提交的jar包的所在路徑
		job.setJarByClass(JobSubmitterLinuxYarn.class);

		job.setMapperClass(SkewWordCountMapper.class);
		job.setReducerClass(SkewWordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 設置map task端的局部聚合邏輯類
		job.setCombinerClass(SkewWordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/mapreduce_test/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/mapreduce_test/wordcount/skew_out"));

		job.setNumReduceTasks(3);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
