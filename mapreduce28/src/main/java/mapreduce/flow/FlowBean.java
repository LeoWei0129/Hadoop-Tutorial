package mapreduce.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * 本案例的功能：演示自定義數據類型如何實現hadoop的序列化接口
 * 1. 該類一定要保留空參constructor
 * 2. write方法中樞出自段二進制數據的順序要與readFiels方法讀取數據的順序一致
 * 
 * 因為FlowBean會作為Map task的輸出傳給Reduce task，既然要傳輸資料，就必須對這個類進行序列化，所以就必須讓這個類
 * 實現序列化接口，Hadoop中序列化的interface是Writable
 * @author Leo
 * @version 1.0
 * @date 2019年10月7日 上午12:25:38
 * @remarks TODO
 *
 */

public class FlowBean implements Writable{
	private int upFlow;
	private int downFlow;
	private int totalFlow;
	private String phone;
	
	/*
	 * 反序列化時，會先調這個constructor，再調readFields()
	 */
	public FlowBean() {
		
	}
	
	public FlowBean(int upFlow, int downFlow, String phone) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.totalFlow = upFlow + downFlow;
		this.phone = phone;
	}
	
	public int getUpFlow() {
		return upFlow;
	}
	
	public void setUpFlow(int upFlow) {
		this.upFlow = upFlow;
	}
	
	public int getDownFlow() {
		return downFlow;
	}
	
	public void setDownFlow(int downFlow) {
		this.downFlow = downFlow;
	}
	
	public int getTotalFlow() {
		return totalFlow;
	}
	
	public String getPhone() {
		return phone;
	}

	/**
	 * Hadoop系統在序列化該類的對象時要調用的方法
	 * 會將該類存的值拿出來，轉成二進制，將這些值放進Hadoop所創建的輸出流out
	 * 最後Hadoop將這個流發送出去給reduce
	 * 該方法為在Writable中需要被implement的方法
	 * @param out
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException { 
		// TODO Auto-generated method stub
		out.writeInt(upFlow); // 將這個變數進行序列化，轉成Hadoop認得的資料序列化格式
		out.writeInt(downFlow);
		out.writeInt(totalFlow);
		out.writeUTF(phone); // 比使用out.write(phoen.getBytes())好
	}
	
	/**
	 * Hadoop系統在反序列化該類的對象時要調用的方法
	 * 可將輸入流in中的值取出來，並再將這些值付給upFlow、downFlow、totalFlow
	 * @param in
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		// 輸入流in會按照拿到的數據(以byte的形式)從頭開始取出byte，所以要out在寫入的順序來read資料
		// readInt()會一次取出4個byte(因為int的大小就是4 bytes)，最後的readUTF()會先讀前兩個byte
		// 這兩個byte指出這個String佔有的byte數n，接著，再根據這個數量讀取接下來的n個byte
		// 而readByte()不會有這種的數量告知，若把out.write()放在寫值時中間，就不好判別接下來要娶幾個byte
		this.upFlow = in.readInt(); 
		this.downFlow = in.readInt();
		this.totalFlow = in.readInt();
		this.phone = in.readUTF();
	}
	
	@Override
	public String toString() {
		return this.upFlow + ", " + this.downFlow + ", " + this.totalFlow + ", " + this.phone;
	}
}
