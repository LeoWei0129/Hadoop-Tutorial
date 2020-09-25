package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN: 是map task讀取到的數據的key的類型，是一行的起始偏移量Long
 * VALUEIN: 是map task讀取到的數據的value的類型，是一行的內容String
 * KEYOUT: 是用戶的自定義map方法要返回的結果(key-value)數據的key的類型，在wordcount邏輯中，需要返回的是單詞String
 * VALUEOUT: 是用戶的自定義map方法要返回的結果(key-value)數據的vlaue類型，在wordcount邏輯中，我們需要返回的是整數Integer
 * 
 * 但是，在mapreduce中，map產生的數據需要傳輸給reduce，就需要進行序列化和反序列化，而jdk中的原生序列化機制產生的數據量比較冗餘，
 * 就會導致數據在mapreduce運行過程中效率低下，所以，hadoop專門設計了自己的序列化機制，那麼，mapreduce中傳輸的數據類型就必須實
 * 現hadoop自己的序列化接口
 * (EX): Long只實現原生jdk的Serialize接口，沒有實現hadoop的序列化接口，Long也就無法被hadoop給序列化而使用，所以需要一個專門給hadoop序列化的數據類型，
 *       這是透過hadoop封裝Long，以實現hadoop序列化接口
 *  hadoop為jdk的常用基本類型Long、String、Integer、Float等數據類型封裝 了自己所實現的hadoop序列化接口類型: LongWriteable、Text、IntWritable、FloatWritable  
 * 
 * 
 * @author Leo
 * @version 1.0
 * @date 2019年9月28日 上午11:07:14
 * @remarks TODO
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
//		super.map(key, value, context); 目前的案例不要去實現父類的map()
		// 切單詞
		String line = value.toString(); // 該行的value因為類別是Text，所以要轉成String，才能做split()
		String[] words = line.split(" ");
		
		for (String word : words) {
			// 因為Mapper類別的返回類型是Text和IntWritable，所以要對原始類型作包裝後才write進context
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
