package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 設置map task端的局部聚合邏輯類
 * @author Leo
 * @version 1.0
 * @date 2020年7月16日 上午1:47:21
 * @remarks TODO
 *
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	// 要告訴hadoop系統要調用此reduce()的是map task
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int count = 0;
		
		for(IntWritable value : values) {
			count += 1;
		}
		
		context.write(key, new IntWritable(count));
	}
}
