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
	 * reduce()處理完所有數據後，reduce task會調用cleanup()
	 */
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// 透過context取得configuration，再從configuration取得設定的參數
		// 這樣topn就不會在這個方法內寫死
		Configuration conf = context.getConfiguration();
		int topn = conf.getInt("top.n", 5); // 5是默認值
		
		int i = 0;
		Set<Entry<PageCount, Object>> entrySet = treeMap.entrySet();
		
		for (Entry<PageCount, Object> entry : entrySet) {
			// cleanup()結束後，reduce task沒有再調用其他方法，把數據返回給reduce task(透過context)
			// entry.getKey()返回pageCount物件
			context.write(new Text(entry.getKey().getPage()), 
					new IntWritable(entry.getKey().getCount()));
			i++;
			if(i == topn) // 只寫前5個進context(根據treeMap)
				return;
		}
	}
}
