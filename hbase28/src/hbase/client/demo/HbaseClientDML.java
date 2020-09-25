package hbase.client.demo;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * DML�ާ@
 */

public class HbaseClientDML {
	Connection conn = null;

	@Before
	public void getConn() throws Exception {
		// �c�ؤ@�ӳs����H
		Configuration conf = HBaseConfiguration.create(); // �|�۰ʥ[��hbase-site.xml
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181"); // hbase�Ȥ�ݭn�Mzookeeper��regionserver�s���A���i��zookeeper�b���x�����W

		conn = ConnectionFactory.createConnection(conf);
	}

	/**
	 * �s�W�ƾڡB�ק�ƾ�(put�л\) �ϥ�for�M���h��put�A�`�����J�j�q�ƾ�
	 */
	@Test
	public void testPutData() throws Exception {
		// �c�ؤ@�Ӿާ@���w��table��H�A�i��DML�ާ@
		Table table = conn.getTable(TableName.valueOf("user_info"));

		// �������c�y�n���J���ƾڬ��@��Put��������H�A�@��Put��H�u�����@��rowkey
		// �Yrowkey�w�s�b�A�S�n���J�o���ƾڡA��ƾڷ|�Q�л\
		Put put1 = new Put(Bytes.toBytes("001")); // �ǵ�toBytes()���Ѽƥi�H�Ointeger��String
		put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("Leo"));
		put1.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
		put1.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("Taipei"));

		Put put2 = new Put(Bytes.toBytes("002"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("Ken"));
		put2.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		put2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"), Bytes.toBytes("Tainan"));

		ArrayList<Put> puts = new ArrayList<Put>();
		puts.add(put1);
		puts.add(put2);

		// ���J�ƾ�
		table.put(puts);

		table.close();
		conn.close();
	}

	/**
	 * �R���ƾ�
	 */
	@Test
	public void testDeleteData() throws Exception {
		Table table = conn.getTable(TableName.valueOf("user_info"));

		// �c�y�@�ӹ�H�ʸ˥H�R���ƾګH��
		Delete delete1 = new Delete(Bytes.toBytes("001"));

		Delete delete2 = new Delete(Bytes.toBytes("002"));
		delete2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"));

		ArrayList<Delete> dels = new ArrayList<Delete>();
		dels.add(delete1);
		dels.add(delete2);

		// �R���ƾ�
		table.delete(dels);

		table.close();
		conn.close();
	}

	/**
	 * �d�߼ƾ�(��@��)
	 */
	@Test
	public void testGetData() throws Exception {
		Table table = conn.getTable(TableName.valueOf("user_info"));

		Get get = new Get("002".getBytes());

		Result result = table.get(get);
		byte[] rowkey = result.getRow(); // ��^�ӵ��ƾڪ�rowkey
		CellScanner cellScanner = result.cellScanner(); // ��^�ӦC���Ҧ�cell(key-value pair)

		// �q���G�����Τ���w���Y��column family��key��value�A�u���@�ӭ�
		byte[] value = result.getValue("base_info".getBytes(), "age".getBytes());
		System.out.println(new String(value));

		System.out.println("----------------------------");

		// �M����C���G�����Ҧ��椸��
		while (cellScanner.advance()) {
			Cell cell = cellScanner.current();
			byte[] rowArray = cell.getRowArray(); // ��kv���ݪ�row�r�`�Ʋ�
			byte[] familyArray = cell.getFamilyArray(); // �C�ڦW���r�`�Ʋ�
			byte[] qualifierArray = cell.getQualifierArray(); // �C�W���r�`�Ʋ�
			byte[] valueArray = cell.getValueArray(); // �Ȫ��r�`�Ʋ�

			// �W�����X�Ӫ����ȬO�ӭȡA�٦���L����ӭȪ����a�T���A�ҥH�ϥΰ_�l�����q�P�W�٪��ר��o��ڦW��
			System.out.println("Rowkey: " + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
			System.out.println(
					"Column Family: " + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
			System.out.println(
					"Key: " + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
			System.out.println("Value: " + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
		}

		table.close();
		conn.close();
	}

	/**
	 * �d�߼ƾ�(�h���A������d��)
	 */
	@Test
	public void testScanData() throws Exception {
		Table table = conn.getTable(TableName.valueOf("user_info"));

		// ���y001��004��rowkey�A�]�t�_�lrowkey�A���]�t����rowkey�A005�[�W�@�Ӥ��i���r�`\000�A�i�H�]�t����rowkey
		Scan scan = new Scan("001".getBytes(), "005\000".getBytes());

		// Scanner�Q�ʸ˦����N���~�i�H�]while loop
		ResultScanner resultScanner = table.getScanner(scan);
		Iterator<Result> iterator = resultScanner.iterator();

		// �M���Ҧ�rowkey
		while (iterator.hasNext()) {
			Result result = iterator.next();
			CellScanner cellScanner = result.cellScanner();

			// �M���Y��rowkey���Ҧ�key-value pair
			while (cellScanner.advance()) {
				Cell cell = cellScanner.current();
				byte[] rowArray = cell.getRowArray(); // ��kv���ݪ�row�r�`�Ʋ�
				byte[] familyArray = cell.getFamilyArray(); // �C�ڦW���r�`�Ʋ�
				byte[] qualifierArray = cell.getQualifierArray(); // �C�W���r�`�Ʋ�
				byte[] valueArray = cell.getValueArray(); // �Ȫ��r�`�Ʋ�

				// �W�����X�Ӫ����ȬO�ӭȡA�٦���L����ӭȪ����a�T���A�ҥH�ϥΰ_�l�����q�P�W�٪��ר��o��ڦW��
				System.out.println("Rowkey: " + new String(rowArray, cell.getRowOffset(), cell.getRowLength()));
				System.out.println(
						"Column Family: " + new String(familyArray, cell.getFamilyOffset(), cell.getFamilyLength()));
				System.out.println(
						"Key: " + new String(qualifierArray, cell.getQualifierOffset(), cell.getQualifierLength()));
				System.out.println("Value: " + new String(valueArray, cell.getValueOffset(), cell.getValueLength()));
			}
			System.out.println("----------------------------");
		}
		System.out.println("----------------------------");
	}

}