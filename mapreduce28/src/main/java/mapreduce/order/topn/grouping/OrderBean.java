package mapreduce.order.topn.grouping;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//�]���n��topn�A�ҥH�n�����
public class OrderBean implements WritableComparable<OrderBean> {
	private String orderId;
	private String userId;
	private String pdtName;
	private float price;
	private int number;
	private float totalPrice;

	public void setParams(String orderId, String userId, String pdtName, float price, int number){
		this.orderId = orderId;
		this.userId = userId;
		this.pdtName = pdtName;
		this.price = price;
		this.number = number;
		this.totalPrice = this.price * this.number;
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getPdtName() {
		return pdtName;
	}

	public void setPdtName(String pdtName) {
		this.pdtName = pdtName;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	public float getTotalPrice() {
		return totalPrice;
	}

	public void setTotalPrice(float totalPrice) {
		this.totalPrice = totalPrice;
	}

	@Override
	public String toString() {
		return "OrderBean [orderId=" + orderId + ", userId=" + userId + ", pdtName=" + pdtName + ", price=" + price
				+ ", number=" + number + ", totalPrice=" + totalPrice + "]";
	}

	/**
	 * Hadoop�t�Φb�ϧǦC�Ƹ�������H�ɭn�եΪ���k
	 * �i�N��J�yin������Ū�X�ӡA�æA�N�o�ǭȥI��upFlow�BdownFlow�BtotalFlow
	 * @param in
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.userId = in.readUTF();
		this.pdtName = in.readUTF();
		this.price = in.readFloat();
		this.number = in.readInt();
		this.totalPrice = this.price * this.number;
	}

	/**
	 * Hadoop�t�Φb�ǦC�Ƹ�������H�ɭn�եΪ���k(�����Qimplement����k)
	 * �|�N�����s���Ȯ��X�ӡA�ন�G�i��A�N�o�ǭȩ�iHadoop�ҳЫت���X�yout
	 * �̫�Hadoop�N�o�Ӭy�o�e�X�h��reduce
	 * �Ӥ�k���bWritable���ݭn�Qimplement����k
	 * @param out
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.pdtName);
		out.writeFloat(this.price);
		out.writeInt(this.number);
		// out.writeFloat(this.totalPrice); // �]���i�H�z�Lprice * number�o���`���AtotalPrice�i���g�L�ǦC��
	}

	/**
	 * �w�]���p�U�AcompareTo(OrderBean o)�񪺬O��H�������j�p
	 * �ҥHorder2 > order1
	 * �o�̭����n�����ӹ�H������orderId�A�p�GorderId�ۦP�A�A���totalPrice
	 * �H���Ӱ��ƧǡA�Φ��p�U���榡(order_1, 800), (order_1, 600), (order_1, 500), (order_3, 800), (order_3, 400)
	 * @param o �M��e������n�������H
	 * @return
	 */
	public int compareTo(OrderBean o) {
		return this.orderId.compareTo(o.getOrderId()) == 0? 
				Float.compare(o.getTotalPrice(), this.getTotalPrice()):
					this.orderId.compareTo(o.getOrderId());
	}

}
