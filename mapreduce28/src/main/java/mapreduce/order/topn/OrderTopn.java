package mapreduce.order.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class OrderTopn {
    // order-id作為key，後面的一串數據做為value
    // 所以針對這個value，用一個自建的class對象來保存比較好
    public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
		// maptask進來後此類(OrderTopnMapper)後，會先實例化orderBean一次
		// 之後再多次調用map()，但只會有一個orderBean，我們只需要在調用map()期間一直改orderBean裡面的值就好
    	OrderBean orderBean = new OrderBean();
		Text text = new Text();

		/**
		 * maptask會每讀一行調用一次map()，造成orderBean、Text被實例化多次，生成大量對象
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			//OrderBean orderBean = new OrderBean();
			orderBean.setParams(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]),
					Integer.parseInt(fields[4]));
			//context.write(new Text(orderBean.getOrderId()), orderBean);
			text.set(orderBean.getOrderId());

			// 此方法在調用時，會將物件序列化並寫入再傳輸出去保存到文件中
			// 序列化後會是二進制格式的數據，已經與原本的物件無關
			// 因此每次orderBean和text在調用set()改值後，寫入context的都是不同的內容
			// 而不會像一般在做ArrayList在持續調用set()和add()方法的時候，只是保存了ArrayList的引用
			// 導致最後輸出ArrayList的數據都是最後保存於ArrayList的數據
			// (可參考ObjectSerializeToFileTest.java)
			// 總結: 從這裡交給maptask的kv對象，會被maptask序列化後存儲，所以不用擔心覆蓋問題
			context.write(text, orderBean);
		}
	}

	public static class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable>{
		// 因為輸出的是topn數據，可以給key是OrderBean，其中就有金額
		// 又因為已經取得想要的數據，value給NullWritable就好
    	@Override
		protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
			// 取得topn參數
			int topn = context.getConfiguration().getInt("order.top.n", 3);

			// 應該在方法內創建ArrayList
			// 因為是針對相同key的所有組取topn
			// 如果在外面創建，不同key之間的所有組都會共用同個arraylist
			ArrayList<OrderBean> beanList = new ArrayList<OrderBean>();

			// reducetask裡提供的迭代器，每次迭代返回給我們的都是一個對象，只是每一次迭代中，該對象set了不同的值
			// 但每次beanList都被add的是這個對象的引用，所以最後迭代完，beanList裡的每個元素都指向這個對象
			// 但此時該對象保存的都是同一組值
    		for(OrderBean ob : values){
    			// 每次迭代都構造一個新的對象，來存儲本次迭代出來的值
    			OrderBean newBean = new OrderBean();
    			newBean.setParams(ob.getOrderId(), ob.getOrderId(), ob.getPdtName(), ob.getPrice(), ob.getNumber());
				beanList.add(newBean);
			}

    		// 對beanList中的orderBean對象做排序(按照總金額大小倒序排序，相同再比商品名稱)
			Collections.sort(beanList);

    		// 透過for迴圈遍歷輸出
			// topn=3不該寫死，可從context取得
			for(int i = 0; i < topn; i++){
				context.write(beanList.get(i), NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 在Windows Local端運行，就不用設置參數
		// 默認只加載core-default.xml core-site.xml
		Configuration conf = new Configuration();
		conf.setInt("order.top.n", 2);

		Job job = Job.getInstance(conf);

		job.setJarByClass(OrderTopn.class);

		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);

		// 可以設置多個reducetask，因為進入其中一個reducetask的一定是同一組key的數據，不會出錯
		// reducetask對應一個保存的文件
		// 此例中，如果設置兩個reducetask，根據對不同key做hash，order_1、order_3會進入同個reducetask，結果文件有ordre_1、
		// order_3的結果，order_2保存在另一個文件
		// 如果reducetask=3，order_1、order_2、order_3分別由不同的reducetask處理(根據hash)，分別保存在三個結果文件
		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(OrderBean.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/mapreduce_test/ordertopn/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/mapreduce_test/ordertopn/output"));

		job.waitForCompletion(true);
	}
}
