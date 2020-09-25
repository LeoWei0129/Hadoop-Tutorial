package mapreduce.page;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageTopnReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	TreeMap<PageCount, Object> treeMap = new TreeMap<PageCount, Object>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int count = 0;
		PageCount pageCount = new PageCount();
		
		for (IntWritable value : values) {
			count += value.get();
		}
	
		pageCount.set(key.toString(), count);
		treeMap.put(pageCount, null);
	}
	
	/**
	 * reduce()�B�z���Ҧ��ƾګ�Areduce task�|�ե�cleanup()
	 */
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// �z�Lcontext���oconfiguration�A�A�qconfiguration���o�]�w���Ѽ�
		// �o��topn�N���|�b�o�Ӥ�k���g��
		Configuration conf = context.getConfiguration();
		int topn = conf.getInt("top.n", 5); // 5�O�q�{��
		
		int i = 0;
		Set<Entry<PageCount, Object>> entrySet = treeMap.entrySet();
		
		for (Entry<PageCount, Object> entry : entrySet) {
			// cleanup()������Areduce task�S���A�եΨ�L��k�A��ƾڪ�^��reduce task(�z�Lcontext)
			// entry.getKey()��^pageCount����
			context.write(new Text(entry.getKey().getPage()), 
					new IntWritable(entry.getKey().getCount()));
			i++;
			if(i == topn) // �u�g�e5�Ӷicontext(�ھ�treeMap)
				return;
		}
	}
}
