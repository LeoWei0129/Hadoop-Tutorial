package zookeeper.demo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperClientDemo {
	ZooKeeper zooKeeper = null;
	
	@Before
	public void init() throws IOException {
		zooKeeper = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, null);
	}
	
	@Test
	public void testCreate() throws IOException, KeeperException, InterruptedException {
		// 創建一個連接zookeeper的客戶端對象
		// 1. 連接有啟動zookeeper的機器，默認port是2181
		// 2. sessionTimeout: 若超出timeout值，離開session，不再等待zookeeper對數據節點的處理
		// 3. watcher: zookeeper負責監聽，當收到通知後，可以匿名實現一個客戶端對改變的數據節點所需做的業務邏輯的接口，null代表沒有監聽的需求，是一個街口的實現類
		//    可以寫null，代表收到通知後不要做任何事
		// (Note): 主機之間的逗號之後不可有空格，不然後解析成[空格]hdp-02:2181
//		ZooKeeper zooKeeper = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, null);
		
		// 創建數據節點
		// 3. ACL(access control list): 訪問權限，哪種類型的客戶可以訪問該數據節點，不過這個數據節點在這個案例中
		//    是給系統使用的，不是某個特定的客戶(人)，所以給一個開放的權限就好
		// 4. createMode: 數據節點的類型
		// 返回值是所建的數據節點路徑: /eclipse
		String create = zooKeeper.create("/eclipse", "HelloEclipse".getBytes(), 
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		System.out.println(create);
		
		zooKeeper.close();
	}
	
	@Test
	public void testUpdate() throws UnsupportedEncodingException, KeeperException, InterruptedException {
		// 3. version: 不同機器上的zookeeper可能持有不同版本的改數據節點，可以指定要修改哪個版本
		//    -1代表不管version，都要修改
		zooKeeper.setData("/eclipse", "Tracy Mcgrady".getBytes("UTF-8"), -1);
		zooKeeper.close();
	}
	
	@Test
	public void testGet() throws KeeperException, InterruptedException, UnsupportedEncodingException {
		// 2. watch: 是否要監聽
		// 3. stat: 要get哪個版本，null代表最新版本
		byte[] data = zooKeeper.getData("/eclipse", false, null);
		System.out.println(new String(data, "UTF-8"));
		zooKeeper.close();
	}
	
	@Test
	public void testGetListChild() throws KeeperException, InterruptedException {
		// 1. 節點路徑 2. 是否要監聽
		// 注意：返回的結果中只有子節點名字，不帶全路徑
		List<String> children = zooKeeper.getChildren("/aa", false);
		
		for (String child : children) {
			System.out.println(child);
		}
		
		zooKeeper.close();
	}
	
	@Test
	public void testDelete() throws InterruptedException, KeeperException {
		zooKeeper.delete("/aa/aaa", -1);
		zooKeeper.close();
	}
	
	
	
	
	
	
	
	
	
	
	
}
