package mapreduce.page.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * �]��PageCount�n�Q�@��Mapper��key�ǻ���Reducer�A������{Hadoop�w�磌�󪺧ǦC�Ʊ��fWritable�A�p�PText�BIntWritable
 * ���@��interface�s��WritableComparable<T>�i�H�N��Writable, Comparable<T>
 * �]������@��Mapper���ǻ��ȵ�Reducer�A�����P�ɺ����ǻ��ǦC�ơA�H��Reducer�w��key�������ƧǾ���(���󤣹��������i�H�۰ʱƧǡA���ۤv��{)
 * @author Leo
 * @version 1.0
 * @date 2019�~10��14�� �U��11:41:56
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
		// o: �n�������H
		// this: ��e��H
		return o.getCount() - this.count == 0? this.page.compareTo(o.getPage()): o.getCount() - this.count;
	}

	// �ǦC��
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.page);
		out.writeInt(this.count);
	}
	
	// �ϧǦC��
	public void readFields(DataInput in) throws IOException {
		this.page = in.readUTF();
		this.count = in.readInt();
	}
	
	public String toString() {
		return this.page + ", " + this.count;
	}
}
