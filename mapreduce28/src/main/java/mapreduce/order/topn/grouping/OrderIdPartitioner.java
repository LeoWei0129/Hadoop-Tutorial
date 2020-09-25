package mapreduce.order.topn.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	
	/**
	 * numPartitions: number of reduce task
	 */
	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		// 按照訂單中的orderId來分區，相同的orderId就會給到同個reduce task
		// 如果是預設的分區規則，會按照key來分區
		// 而原本的key是OrderBean，但每個OrderBean都不一樣，每筆數據都會進到不同分區(但主要還是根據hash決定)
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
	
	

}
