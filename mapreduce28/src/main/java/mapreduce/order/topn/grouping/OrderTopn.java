package mapreduce.order.topn.grouping;

import java.io.IOException;

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

public class OrderTopn {
	public static class OrderTopnMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
		OrderBean bean = new OrderBean();
		NullWritable v = NullWritable.get();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// map()拿到的是一行一行的數據
			String[] fields = value.toString().split(",");
			bean.setParams(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));

			context.write(bean, v);
		}
	}

	public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

		/**
		 * 雖然reduce方法中的參數key只有一個，但是只要迭代器迭代一次，key中的值就會變
		 * 進入到reduce()時，此時這個reducetask要處理的數據已經是按照順序order分組好、totalPrice排序好，比如: (order_1,
		 * 800), (order_1, 600), (order_1, 500) 此時只要按照順序取前三個就好 P.S.
		 * 會進入這個reducetask的所有數據為: (order_1, 800), (order_1, 600), (order_1, 500),
		 * (order_3, 800), (order_3, 400)
		 * 已根據order和totalPrice排序好，是透過OrderBean中的compareTo()實現的
		 */
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values,
				Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			int i = 0;
			for (NullWritable v : values) {
				context.write(key, v);
				i++;
				if (i == 3)
					return;
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
		job.setPartitionerClass(OrderIdPartitioner.class);
		job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
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
