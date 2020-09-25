package mapreduce.order.topn.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分組比較器
 * 此類告訴reduce task，已經被Partitioner決定好要被分發到當前reduce task的所有數據中
 * 哪些數據應該分為同一組，也就是說在每筆數據的key值都不同的情況下(當前的key值是OrderBean對象)。這些不同key的數據應該被分在同一組
 * 然後reduce task會針對這組的數據進行聚合
 * @author Leo
 * @version 1.0
 * @date 2020年7月14日 上午1:42:42
 * @remarks TODO
 *
 */
public class OrderIdGroupingComparator extends WritableComparator {
	/**
	 * reduce task會從文件中取得數據，而這些數據其實是OrderBean對象經過序列化後轉成二進制儲存於文件的結果
	 * 在調用compare(WritableComparable a, WritableComparable b)時，會將reduce task拿到的這個序列化對象
	 * 進行反序列化成Java對象a, b，但WritableComparator不會知道我們想要反序列化成OrderBean類，所以運行時會報錯
	 * 得明確告訴Java，要比較的a, b屬於OrderBean類型，這要透過constructor來實現這個功能
	 */
	public OrderIdGroupingComparator() {
		// OrderBean.class: 要反序列化成哪個類別
		// true: 是否要進行反序列化
		super(OrderBean.class, true);
	}
	
	/**
	 * a, b: 即為兩個OrderBean對象 比如比較a, b: (order_1, 900) & (order_1, 800) or (order_1,
	 * 900) & (order_3, 800)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean o1 = (OrderBean) a;
		OrderBean o2 = (OrderBean) b;

		// 如果o1和o2的orderId相同，返回0，reducetask認為他們是同一組，在做聚合時會放在一起
		return o1.getOrderId().compareTo(o2.getOrderId());
	}
}
