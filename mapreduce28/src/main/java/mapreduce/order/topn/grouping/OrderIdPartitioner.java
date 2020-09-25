package mapreduce.order.topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	
	/**
	 * numPartitions: number of reduce task
	 */
	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		// ���ӭq�椤��orderId�Ӥ��ϡA�ۦP��orderId�N�|����P��reduce task
		// �p�G�O�w�]�����ϳW�h�A�|����key�Ӥ���
		// �ӭ쥻��key�OOrderBean�A���C��OrderBean�����@�ˡA�C���ƾڳ��|�i�줣�P����(���D�n�٬O�ھ�hash�M�w)
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
	
	

}
