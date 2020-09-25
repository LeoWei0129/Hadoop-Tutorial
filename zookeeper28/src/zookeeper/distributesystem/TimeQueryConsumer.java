package zookeeper.distributesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * 
 * 客戶端類別
 * 
 * @author Leo
 * @version 1.0
 * @date 2019年10月23日 下午11:36:11
 * @remarks TODO
 *
 */

public class TimeQueryConsumer {
	ZooKeeper zk = null;

	// 定義一個list用於存放最新的在線服務器列表
	private volatile ArrayList<String> onlineServers = new ArrayList<String>();

	// 構造zk連接對象
	public void connectZooKeeper() throws IOException {
		zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {
			/**
			 * 這是一個回調函數，註冊監聽的過程中，若zk裡的數據節點發生改變，就會呼叫這個method
			 * 我們希望在這個zk中有發生改變時，要查詢在線的服務器有哪些，所以要在這個回調函數call getOnlineServers()
			 * 而當呼叫上述的method後，就又會再註冊監聽一次(因為zk.getChildren()的關係)，就這樣反覆循環，達到持續監聽的機制
			 */
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeChildrenChanged) {
					try {
						// 事件回調邏輯中，在次查詢zk上的再線服務器節點即可，查詢邏輯中又再次註冊了子節點變化事件監聽
						getOnlineServers();
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

	// 查詢在線服務器列表
	public void getOnlineServers() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren("/servers", true); // 掉了這個方法，會產生一個thread負責監聽zk的變化
		ArrayList<String> servers = new ArrayList<String>();

		for (String child : children) {
			byte[] data = zk.getData("/servers/" + child, false, null);
			String hostInfo = new String(data);
			servers.add(hostInfo);
		}

		onlineServers = servers; // 在for外面再賦值給onlineServers，以避免重複的值加進onlineServers
		System.out.println("查詢了一次zk，當前在線的服務器有: " + servers);
	}

	// 處理業務(向一台服務器發送時間查詢請求)
	public void sendRequest() throws InterruptedException, UnknownHostException, IOException {
		// 這兩行若是業務邏輯，必須使用thread.sleep()，避免zk產生的監聽thread隨著主程序的節數也掛掉
		// System.out.println("消費者開始處理業務功能...");
		// Thread.sleep(Long.MAX_VALUE);

		Random random = new Random();

		while (true) {
			try {
				// 挑選一台當前再線的服務器
				int nextInt = random.nextInt(onlineServers.size());
				String server = onlineServers.get(nextInt); //onlineServers更新(調用getOnlineServers())時可能會有延遲在get時就有可能報錯，所以加try-catch
				String hostname = server.split(":")[0];
				int port = Integer.parseInt(server.split(":")[1]);

				Socket socket = new Socket(hostname, port); // 向服務器要發出request的話，使用socket
				OutputStream out = socket.getOutputStream(); // 客戶端發出的請求
				InputStream in = socket.getInputStream(); // 客戶端收到的請求

				out.write("Hello".getBytes());
				out.flush();

				byte[] buf = new byte[256];
				int read = in.read(buf);
				System.out.println("服務器響應的時間為:" + new String(buf, 0, read));

				out.close();
				in.close();
				socket.close();

				Thread.sleep(2000); // 每兩秒查一次
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		TimeQueryConsumer consumer = new TimeQueryConsumer();

		// 構造zk連接對象
		consumer.connectZooKeeper();
		// 查詢在線服務器列表
		consumer.getOnlineServers();
		// 處理業務(向一台服務器發送時間查詢請求)
		consumer.sendRequest();
	}
}
