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
 * 1. 構建連接
 * 2. 從連接中取道一個表DDL操作工具admin
 * 3. admin.createTable(表描述對象);
 * 4. admin.disableTalbe(表名);
 * 5. admin.deleteTalbe(表名);
 * 6. admin.modifyTalbe(表名, 表描述對象);
 * @author Leo
 * @version 1.0
 * @date 2019年11月18日 上午12:35:58
 * @remarks TODO
 *
 */

public class HbaseClientDDL {
	Connection conn = null;
	
	@Before
	public void getConn() throws IOException {
		// 構造一個連結對象
		Configuration conf = HBaseConfiguration.create(); // 會自動加載hbase-site.xml
		// hbase客戶端會先和zookeeper溝通，再和regionserver
		conf.set("hbase.zookeeper.quorum", "hdp-01:2181,hdp-02:2181,hdp-03:2181");
		conn = ConnectionFactory.createConnection(conf);
	}
	/**
	 * DDL: 新增表
	 * @throws Exception 
	 */
	@Test
	public void testCreateTable() throws Exception {
		// 從連接中構造一個DDL操作器
		Admin admin = conn.getAdmin();
		
		// 創建一個表定義描述對象，這一行只給表名
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info2"));
		
		// 創建列族一個表定義
		// 列族不僅給名稱就好，也可以設置其他參數，所以Hbase把列族封裝成一個類
		HColumnDescriptor hColumnDescriptor_1 = new HColumnDescriptor("base_info");
		hColumnDescriptor_1.setMaxVersions(3); // 設置該列族中存儲數據的最大版本數，默認是1
		HColumnDescriptor hColumnDescriptor_2 = new HColumnDescriptor("extra_info");
		
		// 將列族定義信息對象放入表定義對象中
		hTableDescriptor.addFamily(hColumnDescriptor_1);
		hTableDescriptor.addFamily(hColumnDescriptor_2);
		
		// 用DDL操作器對象admin來建表
		admin.createTable(hTableDescriptor);
		
		// 關閉連接
		admin.close();
		conn.close();
	}
	
	/**
	 * DDL: 刪除表
	 * @throws IOException 
	 * @throws Exception 
	 */
	@Test
	public void testDropTable() throws IOException {
		Admin admin = conn.getAdmin();
		
		// 必須先停用表，再刪除表
		admin.disableTable(TableName.valueOf("user_info"));
		admin.deleteTable(TableName.valueOf("user_info"));
		
		admin.close();
		conn.close();
	}
	
	/**
	 * DDL: 修改表
	 * @throws IOException 
	 * @throws Exception 
	 */
	@Test
	public void testAlterTable() throws IOException {
		Admin admin = conn.getAdmin();
		
		// 取出舊的表定義
		HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info"));
		
		// 新構造一個列族定義
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info"); 
		// 為列族設置參數
		hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL); // 設置該列族的布隆過濾器類型
 		
		// 將列族定義添加到表定義對象中
		hTableDescriptor.addFamily(hColumnDescriptor);
		
		// 將修改過的表定義交給admin去提交
		admin.modifyTable(TableName.valueOf("user_info"), hTableDescriptor);
		
		admin.close();
		conn.close();
	}
	
	
	/**
	 * DML
	 */
	
}
