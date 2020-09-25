package zookeeper.demo;

import java.io.IOException;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperWatch {
	ZooKeeper zooKeeper = null;

	@Before
	public void init() throws IOException {
		// 創建一個連接zookeeper的客戶端對象
		// 客戶端對象回調時的watcher，watcher就不用寫在getData()裡，直接船個true給這個method就好
		zooKeeper = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// 避免在初始化節點時也調用了主要的業務邏輯(我們只要數據節點發生改變時才調用這個方法)
				if(event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeDataChanged) {
					System.out.println(event.getPath()); // 收到的事件所發生的數據節點路徑
					System.out.println(event.getType()); // 收到的事件的類型
					System.out.println("此方法是實作收到通知後所要處理的業務邏輯"); // 收到事件後，我們的處理邏輯
				
					// 註冊一次監聽，當數據節點發生改變，當前的這個監聽就會被註銷，就必須再註冊一次監聽
					// 下面的寫法可以確保反覆監聽
					// 原本是zooKeeper註冊監聽，收到通知後會調用process()方法，又會在這裡註冊監聽一次，所以可以達到反覆監聽
					try {
						zooKeeper.getData("/eclipse", true, null);
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		});
	}
	
	@Test
	public void testGetWatch() throws Exception {
		// 開啟監聽功能
		// zooKeeper執行完getData()後，會再啟動一個線程負責監聽
		// Thread.sleep()負責不讓這個method結束，不然zooKeeper也會跟著被回收，就不會再監聽了
		// 2: 可以是true，代表要監聽，或是一個接口的實現watcher，代表收到數據節點有改變的通知後，要做的處理
		// new Watcher()是zookeeper收到改變通知後的一個回調邏輯(rollback)，創建客戶端時就寫watcher
		// 在每次掉用這個方法時，也都會調用watcher(在有改變時)
		byte[] data = zooKeeper.getData("/eclipse", true, null); // 監聽節點數據變化
//		byte[] childData = zooKeeper.getData("/aa/aab", true, null); // 監聽節點的子節點變化事件
		System.out.println(new String(data, "UTF-8"));
		Thread.sleep(Long.MAX_VALUE); // 目前這個線程(方法)再sleep，但zookeeper又創建的另一個線程持續監聽
//		zooKeeper.close(); // 現在zooKeeper要負責監聽，所以不可以close
	}
}
