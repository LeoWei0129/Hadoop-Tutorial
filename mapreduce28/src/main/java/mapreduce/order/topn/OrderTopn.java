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
    // order-id�@��key�A�᭱���@��ƾڰ���value
    // �ҥH�w��o��value�A�Τ@�Ӧ۫ت�class��H�ӫO�s����n
    public static class OrderTopnMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
		// maptask�i�ӫᦹ��(OrderTopnMapper)��A�|����Ҥ�orderBean�@��
		// ����A�h���ե�map()�A���u�|���@��orderBean�A�ڭ̥u�ݭn�b�ե�map()�����@����orderBean�̭����ȴN�n
    	OrderBean orderBean = new OrderBean();
		Text text = new Text();

		/**
		 * maptask�|�CŪ�@��եΤ@��map()�A�y��orderBean�BText�Q��ҤƦh���A�ͦ��j�q��H
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

			// ����k�b�եήɡA�|�N����ǦC�ƨüg�J�A�ǿ�X�h�O�s����
			// �ǦC�ƫ�|�O�G�i��榡���ƾڡA�w�g�P�쥻������L��
			// �]���C��orderBean�Mtext�b�ե�set()��ȫ�A�g�Jcontext�����O���P�����e
			// �Ӥ��|���@��b��ArrayList�b����ե�set()�Madd()��k���ɭԡA�u�O�O�s�FArrayList���ޥ�
			// �ɭP�̫��XArrayList���ƾڳ��O�̫�O�s��ArrayList���ƾ�
			// (�i�Ѧ�ObjectSerializeToFileTest.java)
			// �`��: �q�o�̥浹maptask��kv��H�A�|�Qmaptask�ǦC�ƫ�s�x�A�ҥH���ξ���л\���D
			context.write(text, orderBean);
		}
	}

	public static class OrderTopnReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable>{
		// �]����X���Otopn�ƾڡA�i�H��key�OOrderBean�A�䤤�N�����B
		// �S�]���w�g���o�Q�n���ƾڡAvalue��NullWritable�N�n
    	@Override
		protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
			// ���otopn�Ѽ�
			int topn = context.getConfiguration().getInt("order.top.n", 3);

			// ���Ӧb��k���Ы�ArrayList
			// �]���O�w��ۦPkey���Ҧ��ը�topn
			// �p�G�b�~���ЫءA���Pkey�������Ҧ��ճ��|�@�ΦP��arraylist
			ArrayList<OrderBean> beanList = new ArrayList<OrderBean>();

			// reducetask�̴��Ѫ����N���A�C�����N��^���ڭ̪����O�@�ӹ�H�A�u�O�C�@�����N���A�ӹ�Hset�F���P����
			// ���C��beanList���Qadd���O�o�ӹ�H���ޥΡA�ҥH�̫᭡�N���AbeanList�̪��C�Ӥ��������V�o�ӹ�H
			// �����ɸӹ�H�O�s�����O�P�@�խ�
    		for(OrderBean ob : values){
    			// �C�����N���c�y�@�ӷs����H�A�Ӧs�x�������N�X�Ӫ���
    			OrderBean newBean = new OrderBean();
    			newBean.setParams(ob.getOrderId(), ob.getOrderId(), ob.getPdtName(), ob.getPrice(), ob.getNumber());
				beanList.add(newBean);
			}

    		// ��beanList����orderBean��H���Ƨ�(�����`���B�j�p�˧ǱƧǡA�ۦP�A��ӫ~�W��)
			Collections.sort(beanList);

    		// �z�Lfor�j��M����X
			// topn=3���Ӽg���A�i�qcontext���o
			for(int i = 0; i < topn; i++){
				context.write(beanList.get(i), NullWritable.get());
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
