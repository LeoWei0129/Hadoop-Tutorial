package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * �]�mmap task�ݪ������E�X�޿���
 * @author Leo
 * @version 1.0
 * @date 2020�~7��16�� �W��1:47:21
 * @remarks TODO
 *
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
	// �n�i�Dhadoop�t�έn�եΦ�reduce()���Omap task
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
