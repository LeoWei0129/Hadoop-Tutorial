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
 * DML操作
 */

public class HbaseClientDML {
	Connection conn = null;

	@Before
	public void getConn() throws Exception {
		// 構建一個連接對象
		Configuration conf = HBaseConfiguration.create(); // 會自動加載hbase-site.xml
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181"); // hbase客戶端要和zookeeper及regionserver連接，須告知zookeeper在哪台機器上

		conn = ConnectionFactory.createConnection(conf);
	}

	/**
	 * 新增數據、修改數據(put覆蓋) 使用for遍歷多次put，循環插入大量數據
	 */
	@Test
	public void testPutData() throws Exception {
		// 構建一個操作指定表的table對象，進行DML操作
		Table table = conn.getTable(TableName.valueOf("user_info"));

		// 必須先構造要插入的數據為一個Put類型的對象，一個Put對象只對應一個rowkey
		// 若rowkey已存在，又要插入這筆數據，原數據會被覆蓋
		Put put1 = new Put(Bytes.toBytes("001")); // 傳給toBytes()的參數可以是integer或String
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

		// 插入數據
		table.put(puts);

		table.close();
		conn.close();
	}

	/**
	 * 刪除數據
	 */
	@Test
	public void testDeleteData() throws Exception {
		Table table = conn.getTable(TableName.valueOf("user_info"));

		// 構造一個對象封裝以刪除數據信息
		Delete delete1 = new Delete(Bytes.toBytes("001"));

		Delete delete2 = new Delete(Bytes.toBytes("002"));
		delete2.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("address"));

		ArrayList<Delete> dels = new ArrayList<Delete>();
		dels.add(delete1);
		dels.add(delete2);

		// 刪除數據
		table.delete(dels);

		table.close();
		conn.close();
	}

	/**
	 * 查詢數據(單一筆)
	 */
	@Test
	public void testGetData() throws Exception {
		Table table = conn.getTable(TableName.valueOf("user_info"));

		Get get = new Get("002".getBytes());

		Result result = table.get(get);
		byte[] rowkey = result.getRow(); // 返回該筆數據的rowkey
		CellScanner cellScanner = result.cellScanner(); // 返回該列的所有cell(key-value pair)

		// 從結果中取用戶指定的某個column family及key的value，只取一個值
		byte[] value = result.getValue("base_info".getBytes(), "age".getBytes());
		System.out.println(new String(value));

		System.out.println("----------------------------");

		// 遍歷整列結果中的所有單元格
		while (cellScanner.advance()) {
			Cell cell = cellScanner.current();
			byte[] rowArray = cell.getRowArray(); // 該kv所屬的row字節數組
			byte[] familyArray = cell.getFamilyArray(); // 列族名的字節數組
			byte[] qualifierArray = cell.getQualifierArray(); // 列名的字節數組
			byte[] valueArray = cell.getValueArray(); // 值的字節數組

			// 上面取出來的不僅是該值，還有其他關於該值的附帶訊息，所以使用起始偏移量與名稱長度取得實際名稱
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
	 * 查詢數據(多筆，按行鍵範圍)
	 */
	@Test
	public void testScanData() throws Exception {
		Table table = conn.getTable(TableName.valueOf("user_info"));

		// 掃描001到004的rowkey，包含起始rowkey，不包含結束rowkey，005加上一個不可見字節\000，可以包含結束rowkey
		Scan scan = new Scan("001".getBytes(), "005\000".getBytes());

		// Scanner被封裝成迭代器才可以跑while loop
		ResultScanner resultScanner = table.getScanner(scan);
		Iterator<Result> iterator = resultScanner.iterator();

		// 遍歷所有rowkey
		while (iterator.hasNext()) {
			Result result = iterator.next();
			CellScanner cellScanner = result.cellScanner();

			// 遍歷某筆rowkey的所有key-value pair
			while (cellScanner.advance()) {
				Cell cell = cellScanner.current();
				byte[] rowArray = cell.getRowArray(); // 該kv所屬的row字節數組
				byte[] familyArray = cell.getFamilyArray(); // 列族名的字節數組
				byte[] qualifierArray = cell.getQualifierArray(); // 列名的字節數組
				byte[] valueArray = cell.getValueArray(); // 值的字節數組

				// 上面取出來的不僅是該值，還有其他關於該值的附帶訊息，所以使用起始偏移量與名稱長度取得實際名稱
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