package mapreduce.flow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Reducer�x�����e��ӰѼƫ��O�A�n����mapper��context.write()���Ѽƫ��O����
 * �NReducer�g�i�ɮת��ȥHFlowBean�ʸ˰_�ӡA�i�H�A�z�LtoString()���o�o�ǭ�(�ĥ|�Ӫx���Ѽ�)
 * @author Leo
 * @version 1.0
 * @date 2019�~10��7�� �U��9:53:48
 * @remarks TODO
 *
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	/**
	 * key: �Y�ӹq�ܸ��X�A�o�䪺key�w�g�O��Mapper�ǹL�Ӫ�phone�F
	 * values: �o�ӹq�ܸ��X�Ҳ��ͪ��Ҧ��X�ݰO�������y�q�ƾ�
	 * 
	 * (EX):
	 * <0954123486, flowBean1>, <0954123486, flowBean2>, <0954123486, flowBean3>...
	 */
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// Iterable���foreach�A�ҥH���Φh�@���z�L���o���N��(�z�LhasNext()�ӭ��N)�A�p�U
//		Iterator<FlowBean> iter = values.iterator();
//		while(iter.hasNext()) {
//			// do something...
//		}
		
		int upSum = 0;
		int downSum = 0;
		
		for (FlowBean flowBean : values) {
			upSum += flowBean.getUpFlow();
			downSum += flowBean.getDownFlow();
		}
		
		context.write(new Text(key.toString()), new FlowBean(upSum, downSum, key.toString()));
	}
}
