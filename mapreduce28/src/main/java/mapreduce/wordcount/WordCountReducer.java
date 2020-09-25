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
		// reduce task�O�H�@�ռƲըӽ�reduce()(�P��key���Ҧ�����)
		Iterator<IntWritable> iterator = values.iterator(); // �n���o���N���A�~���for
		int count = 0;
		
		while(iterator.hasNext()) {
			IntWritable value = iterator.next();
			count += value.get();
		}
		
		// �[�`���ƾګ�A�n��ƾڪ�^��reduce task�A��reduce()�S���w�qreturn
		context.write(key, new IntWritable(count));
	}
}
