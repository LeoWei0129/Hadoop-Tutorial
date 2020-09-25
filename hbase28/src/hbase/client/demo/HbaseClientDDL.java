package hbase.client.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * 1. �c�سs��
 * 2. �q�s�������D�@�Ӫ�DDL�ާ@�u��admin
 * 3. admin.createTable(��y�z��H);
 * 4. admin.disableTalbe(��W);
 * 5. admin.deleteTalbe(��W);
 * 6. admin.modifyTalbe(��W, ��y�z��H);
 * @author Leo
 * @version 1.0
 * @date 2019�~11��18�� �W��12:35:58
 * @remarks TODO
 *
 */

public class HbaseClientDDL {
	Connection conn = null;
	
	@Before
	public void getConn() throws IOException {
		// �c�y�@�ӳs����H
		Configuration conf = HBaseConfiguration.create(); // �|�۰ʥ[��hbase-site.xml
		// hbase�Ȥ�ݷ|���Mzookeeper���q�A�A�Mregionserver
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181");
		conn = ConnectionFactory.createConnection(conf);
	}
	/**
	 * DDL: �s�W��
	 * @throws Exception 
	 */
	@Test
	public void testCreateTable() throws Exception {
		// �q�s�����c�y�@��DDL�ާ@��
		Admin admin = conn.getAdmin();
		
		// �Ыؤ@�Ӫ�w�q�y�z��H�A�o�@��u����W
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info2"));
		
		// �ЫئC�ڤ@�Ӫ�w�q
		// �C�ڤ��ȵ��W�ٴN�n�A�]�i�H�]�m��L�ѼơA�ҥHHbase��C�ګʸ˦��@����
		HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");
		hColumnDescriptor_1.setMaxVersions(3); // �]�m�ӦC�ڤ��s�x�ƾڪ��̤j�����ơA�q�{�O1
		HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("extra_info");
		
		// �N�C�کw�q�H����H��J��w�q��H��
		hTableDescriptor.addFamily(hColumnDescriptor_1);
		hTableDescriptor.addFamily(hColumnDescriptor_2);
		
		// ��DDL�ާ@����Hadmin�ӫت�
		admin.createTable(hTableDescriptor);
		
		// �����s��
		admin.close();
		conn.close();
	}
	
	/**
	 * DDL: �R����
	 * @throws IOException 
	 * @throws Exception 
	 */
	@Test
	public void testDropTable() throws IOException {
		Admin admin = conn.getAdmin();
		
		// ���������Ϊ�A�A�R����
		admin.disableTable(TableName.valueOf("user_info"));
		admin.deleteTable(TableName.valueOf("user_info"));
		
		admin.close();
		conn.close();
	}
	
	/**
	 * DDL: �ק��
	 * @throws IOException 
	 * @throws Exception 
	 */
	@Test
	public void testAlterTable() throws IOException {
		Admin admin = conn.getAdmin();
		
		// ���X�ª���w�q
		HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
		
		// �s�c�y�@�ӦC�کw�q
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info"); 
		// ���C�ڳ]�m�Ѽ�
		hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL); // �]�m�ӦC�ڪ������L�o������
 		
		// �N�C�کw�q�K�[���w�q��H��
		hTableDescriptor.addFamily(hColumnDescriptor);
		
		// �N�ק�L����w�q�浹admin�h����
		admin.modifyTable(TableName.valueOf("user_info"), hTableDescriptor);
		
		admin.close();
		conn.close();
	}
	
	
	/**
	 * DML
	 */
	
}
