package mapreduce.order.topn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//因為要取topn，所以要做比較
public class OrderBean implements WritableComparable<OrderBean>, Serializable {
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
	 * Hadoop系統在反序列化該類的對象時要調用的方法
	 * 可將輸入流in中的值讀出來，並再將這些值付給upFlow、downFlow、totalFlow
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
	 * Hadoop系統在序列化該類的對象時要調用的方法(必須被implement的方法)
	 * 會將該類存的值拿出來，轉成二進制，將這些值放進Hadoop所創建的輸出流out
	 * 最後Hadoop將這個流發送出去給reduce
	 * 該方法為在Writable中需要被implement的方法
	 * @param out
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.pdtName);
		out.writeFloat(this.price);
		out.writeInt(this.number);
		// out.writeFloat(this.totalPrice); // 因為可以透過price * number得到總價，totalPrice可不經過序列化
	}

	/**
	 * 預設情況下，compareTo(OrderBean o)比的是對象之間的大小
	 * 所以order2 > order1
	 * 但實際上我們要比的是在一個對象中(e.g. order1)每個商品價格的大小
	 * 所以要implement這個方法，寫具體的比較邏輯
	 * @param o 和當前此物件要比較的對象
	 * @return
	 */
	public int compareTo(OrderBean o) {
		return Float.compare(o.getTotalPrice(), this.getTotalPrice()) == 0?
				o.getPdtName().compareTo(this.getPdtName()): Float.compare(o.getTotalPrice(), this.getTotalPrice());
	}

}
