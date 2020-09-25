package mapreduce.order.topn.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ���դ����
 * �����i�Dreduce task�A�w�g�QPartitioner�M�w�n�n�Q���o���ereduce task���Ҧ��ƾڤ�
 * ���Ǽƾ����Ӥ����P�@�աA�]�N�O���b�C���ƾڪ�key�ȳ����P�����p�U(��e��key�ȬOOrderBean��H)�C�o�Ǥ��Pkey���ƾ����ӳQ���b�P�@��
 * �M��reduce task�|�w��o�ժ��ƾڶi��E�X
 * @author Leo
 * @version 1.0
 * @date 2020�~7��14�� �W��1:42:42
 * @remarks TODO
 *
 */
public class OrderIdGroupingComparator extends WritableComparator {
	/**
	 * reduce task�|�q��󤤨��o�ƾڡA�ӳo�Ǽƾڨ��OOrderBean��H�g�L�ǦC�ƫ��ন�G�i���x�s���󪺵��G
	 * �b�ե�compare(WritableComparable a, WritableComparable b)�ɡA�|�Nreduce task���쪺�o�ӧǦC�ƹ�H
	 * �i��ϧǦC�Ʀ�Java��Ha, b�A��WritableComparator���|���D�ڭ̷Q�n�ϧǦC�Ʀ�OrderBean���A�ҥH�B��ɷ|����
	 * �o���T�i�DJava�A�n�����a, b�ݩ�OrderBean�����A�o�n�z�Lconstructor�ӹ�{�o�ӥ\��
	 */
	public OrderIdGroupingComparator() {
		// OrderBean.class: �n�ϧǦC�Ʀ��������O
		// true: �O�_�n�i��ϧǦC��
		super(OrderBean.class, true);
	}
	
	/**
	 * a, b: �Y�����OrderBean��H ��p���a, b: (order_1, 900) & (order_1, 800) or (order_1,
	 * 900) & (order_3, 800)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean o1 = (OrderBean) a;
		OrderBean o2 = (OrderBean) b;

		// �p�Go1�Mo2��orderId�ۦP�A��^0�Areducetask�{���L�̬O�P�@�աA�b���E�X�ɷ|��b�@�_
		return o1.getOrderId().compareTo(o2.getOrderId());
	}
}
