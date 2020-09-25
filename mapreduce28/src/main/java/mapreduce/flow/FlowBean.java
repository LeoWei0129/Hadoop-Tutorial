package mapreduce.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * ���רҪ��\��G�t�ܦ۩w�q�ƾ������p���{hadoop���ǦC�Ʊ��f
 * 1. �����@�w�n�O�d�Ű�constructor
 * 2. write��k���ϥX�۬q�G�i��ƾڪ����ǭn�PreadFiels��kŪ���ƾڪ����Ǥ@�P
 * 
 * �]��FlowBean�|�@��Map task����X�ǵ�Reduce task�A�J�M�n�ǿ��ơA�N������o�����i��ǦC�ơA�ҥH�N�������o����
 * ��{�ǦC�Ʊ��f�AHadoop���ǦC�ƪ�interface�OWritable
 * @author Leo
 * @version 1.0
 * @date 2019�~10��7�� �W��12:25:38
 * @remarks TODO
 *
 */

public class FlowBean implements Writable{
	private int upFlow;
	private int downFlow;
	private int totalFlow;
	private String phone;
	
	/*
	 * �ϧǦC�ƮɡA�|���ճo��constructor�A�A��readFields()
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
	 * Hadoop�t�Φb�ǦC�Ƹ�������H�ɭn�եΪ���k
	 * �|�N�����s���Ȯ��X�ӡA�ন�G�i��A�N�o�ǭȩ�iHadoop�ҳЫت���X�yout
	 * �̫�Hadoop�N�o�Ӭy�o�e�X�h��reduce
	 * �Ӥ�k���bWritable���ݭn�Qimplement����k
	 * @param out
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException { 
		// TODO Auto-generated method stub
		out.writeInt(upFlow); // �N�o���ܼƶi��ǦC�ơA�নHadoop�{�o����ƧǦC�Ʈ榡
		out.writeInt(downFlow);
		out.writeInt(totalFlow);
		out.writeUTF(phone); // ��ϥ�out.write(phoen.getBytes())�n
	}
	
	/**
	 * Hadoop�t�Φb�ϧǦC�Ƹ�������H�ɭn�եΪ���k
	 * �i�N��J�yin�����Ȩ��X�ӡA�æA�N�o�ǭȥI��upFlow�BdownFlow�BtotalFlow
	 * @param in
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		// ��J�yin�|���Ӯ��쪺�ƾ�(�Hbyte���Φ�)�q�Y�}�l���Xbyte�A�ҥH�nout�b�g�J�����Ǩ�read���
		// readInt()�|�@�����X4��byte(�]��int���j�p�N�O4 bytes)�A�̫᪺readUTF()�|��Ū�e���byte
		// �o���byte���X�o��String������byte��n�A���ۡA�A�ھڳo�ӼƶqŪ�����U�Ӫ�n��byte
		// ��readByte()���|���o�ت��ƶq�i���A�Y��out.write()��b�g�Ȯɤ����A�N���n�P�O���U�ӭn���X��byte
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
