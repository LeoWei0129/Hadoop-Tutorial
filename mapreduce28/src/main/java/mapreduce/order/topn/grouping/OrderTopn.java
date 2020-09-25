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
			// map()���쪺�O�@��@�檺�ƾ�
			String[] fields = value.toString().split(",");
			bean.setParams(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));

			context.write(bean, v);
		}
	}

	public static class OrderTopnReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

		/**
		 * ���Mreduce��k�����Ѽ�key�u���@�ӡA���O�u�n���N�����N�@���Akey�����ȴN�|��
		 * �i�J��reduce()�ɡA���ɳo��reducetask�n�B�z���ƾڤw�g�O���Ӷ���order���զn�BtotalPrice�ƧǦn�A��p: (order_1,
		 * 800), (order_1, 600), (order_1, 500) ���ɥu�n���Ӷ��Ǩ��e�T�ӴN�n P.S.
		 * �|�i�J�o��reducetask���Ҧ��ƾڬ�: (order_1, 800), (order_1, 600), (order_1, 500),
		 * (order_3, 800), (order_3, 400)
		 * �w�ھ�order�MtotalPrice�ƧǦn�A�O�z�LOrderBean����compareTo()��{��
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
		// �bWindows Local�ݹB��A�N���γ]�m�Ѽ�
		// �q�{�u�[��core-default.xml core-site.xml
		Configuration conf = new Configuration();
		conf.setInt("order.top.n", 2);

		Job job = Job.getInstance(conf);

		job.setJarByClass(OrderTopn.class);

		job.setMapperClass(OrderTopnMapper.class);
		job.setReducerClass(OrderTopnReducer.class);

		// �i�H�]�m�h��reducetask�A�]���i�J�䤤�@��reducetask���@�w�O�P�@��key���ƾڡA���|�X��
		// reducetask�����@�ӫO�s�����
		// ���Ҥ��A�p�G�]�m���reducetask�A�ھڹ藍�Pkey��hash�Aorder_1�Border_3�|�i�J�P��reducetask�A���G���ordre_1�B
		// order_3�����G�Aorder_2�O�s�b�t�@�Ӥ��
		// �p�Greducetask=3�Aorder_1�Border_2�Border_3���O�Ѥ��P��reducetask�B�z(�ھ�hash)�A���O�O�s�b�T�ӵ��G���
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
