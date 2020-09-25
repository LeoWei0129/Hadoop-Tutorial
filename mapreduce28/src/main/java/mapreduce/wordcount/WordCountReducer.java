package mapreduce.wordcount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
//		super.reduce(arg0, arg1, arg2);
		// reduce task是以一組數組來調reduce()(同個key的所有的值)
		Iterator<IntWritable> iterator = values.iterator(); // 要取得迭代器，才能用for
		int count = 0;
		
		while(iterator.hasNext()) {
			IntWritable value = iterator.next();
			count += value.get();
		}
		
		// 加總完數據後，要把數據返回給reduce task，但reduce()沒有定義return
		context.write(key, new IntWritable(count));
	}
}
