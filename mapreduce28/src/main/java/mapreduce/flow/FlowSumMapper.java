package mapreduce.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * FlowBean: 自定義的類來封裝多個要進行reduce的值(這個例子: 上行流量、下行流量、總流量)，因為MapReduce沒有
 * 自己的可裝多個值的類別，只好自己定義一個類別來封裝以達此目的
 * @author Leo
 * @version 1.0
 * @date 2019年10月7日 上午12:18:09
 * @remarks TODO
 *
 */

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	/**
	 * key是讀取的每一行中的電話號碼
	 * value是讀取的黑一行中的下行流量和上行流量
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] fields = line.split("\t");
		
		String phone = fields[1];
		int upFlow = Integer.parseInt(fields[fields.length - 3]);
		int downFlow = Integer.parseInt(fields[fields.length - 2]);
		
		// 傳輸出去的key-value pair格式，寫進context中
		context.write(new Text(phone), new FlowBean(upFlow, downFlow, phone));
		
		
	}
}
