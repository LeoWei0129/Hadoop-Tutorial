package mapreduce.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * FlowBean: �۩w�q�����ӫʸ˦h�ӭn�i��reduce����(�o�ӨҤl: �W��y�q�B�U��y�q�B�`�y�q)�A�]��MapReduce�S��
 * �ۤv���i�˦h�ӭȪ����O�A�u�n�ۤv�w�q�@�����O�ӫʸ˥H�F���ت�
 * @author Leo
 * @version 1.0
 * @date 2019�~10��7�� �W��12:18:09
 * @remarks TODO
 *
 */

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	/**
	 * key�OŪ�����C�@�椤���q�ܸ��X
	 * value�OŪ�����¤@�椤���U��y�q�M�W��y�q
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] fields = line.split("\t");
		
		String phone = fields[1];
		int upFlow = Integer.parseInt(fields[fields.length - 3]);
		int downFlow = Integer.parseInt(fields[fields.length - 2]);
		
		// �ǿ�X�h��key-value pair�榡�A�g�icontext��
		context.write(new Text(phone), new FlowBean(upFlow, downFlow, phone));
		
		
	}
}
