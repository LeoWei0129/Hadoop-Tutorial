package mapreduce.page.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * 因為PageCount要被作為Mapper的key傳遞給Reducer，必須實現Hadoop針對物件的序列化接口Writable，如同Text、IntWritable
 * 有一個interface叫做WritableComparable<T>可以代替Writable, Comparable<T>
 * 因為物件作為Mapper的傳遞值給Reducer，必須同時滿足傳遞序列化，以及Reducer針對key的內部排序機制(物件不像基本類型可以自動排序，須自己實現)
 * @author Leo
 * @version 1.0
 * @date 2019年10月14日 下午11:41:56
 * @remarks TODO
 *
 */

public class PageCount implements Writable, Comparable<PageCount>{
	private String page;
	private int count;
	
	public void set(String page, int count) {
		this.page = page;
		this.count = count;
	}
	
	public String getPage() {
		return page;
	}
	public void setPage(String page) {
		this.page = page;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}

	public int compareTo(PageCount o) {
		// TODO Auto-generated method stub
		// o: 要比較的對象
		// this: 當前對象
		return o.getCount() - this.count == 0? this.page.compareTo(o.getPage()): o.getCount() - this.count;
	}

	// 序列化
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.page);
		out.writeInt(this.count);
	}
	
	// 反序列化
	public void readFields(DataInput in) throws IOException {
		this.page = in.readUTF();
		this.count = in.readInt();
	}
	
	public String toString() {
		return this.page + ", " + this.count;
	}
}
