package zookeeper.distributesystem;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 
 * 服務端的主程式
 * @author Leo
 * @version 1.0
 * @date 2019年10月23日 下午11:08:29
 * @remarks TODO
 *
 */

public class TimeQueryServer {
	ZooKeeper zk = null;
	
	// 構造zk客戶端連接
	public void connectZooKeeper() throws IOException {
		zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				
			}});
	}
	
	// 註冊服務器信息
	public void registerServerInfo(String hostname, String port) throws Exception {
		// 先判斷註冊節點的復節點是否存在，如果不存在，則創建
		// 如果在還沒創建父節點的情況下就直接創建子節點，會報錯
		// 2. 是否要監聽這個數據節點，目前不需要，只要判斷他是否存在就好
		// 返回該數據節點的元數據狀態訊息，類型是Stat
		Stat stat = zk.exists("/servers", false);
		if(stat == null) {
			zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		// 註冊服務器數據到zk的約定註冊節點下
		String create = zk.create("/servers/server", (hostname+":"+port).getBytes(), 
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(hostname + " 服務器向zk註冊信息成功，註冊的節點為: " + create);
		
//		zk.close(); // 不要close，close的話，剛註冊上去的節點又會被刪除
	}
	
	public static void main(String[] args) throws Exception {
		TimeQueryServer timeQueryServer = new TimeQueryServer();
		
		// 構造zk客戶端連接
		timeQueryServer.connectZooKeeper();
		
		// 註冊服務器信息
		// 變更hostname或port，就可以再創建另一台服務端機器與zk連接
		timeQueryServer.registerServerInfo(args[0], args[1]);
		
		// 啟動業務線程開始處理業務
		// 此為zk在create數據節點後會啟動的線程，透過port連結zk後此thread
		// 不需要在這裡Thread.sleep()， 因為新產生的thread會在while loop(true)中持續與客戶端保持聯繫
		// 不會退出子線程，所以目前的這個主線程main()也會持續執行line 70，不會退出，創建的zk也不會掛掉
		new TimeQueryService(Integer.parseInt(args[1])).start();
	}
}
