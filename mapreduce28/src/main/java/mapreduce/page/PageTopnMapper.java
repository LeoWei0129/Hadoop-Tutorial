package mapreduce.page;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 1. LongWritable: 每行起始偏移量
 * 2. Text: 每一行
 * 3. Text: page頁面的url
 * 4. IntWritable: page的次數
 * 3和4要傳輸給reduce task的key-value對
 * 輸入檔案的每行格式順序: 日期        page的url
 * @author Leo
 * @version 1.0
 * @date 2019年10月9日 下午11:03:49
 * @remarks TODO
 *
 */

public class PageTopnMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(" ");
		context.write(new Text(split[1]), new IntWritable(1)); // key-value pair
	}
}
