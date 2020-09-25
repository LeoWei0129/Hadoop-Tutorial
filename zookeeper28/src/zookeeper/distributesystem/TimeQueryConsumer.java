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
 * �Ȥ�����O
 * 
 * @author Leo
 * @version 1.0
 * @date 2019�~10��23�� �U��11:36:11
 * @remarks TODO
 *
 */

public class TimeQueryConsumer {
	ZooKeeper zk = null;

	// �w�q�@��list�Ω�s��̷s���b�u�A�Ⱦ��C��
	private volatile ArrayList<String> onlineServers = new ArrayList<String>();

	// �c�yzk�s����H
	public void connectZooKeeper() throws IOException {
		zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {
			/**
			 * �o�O�@�Ӧ^�ը�ơA���U��ť���L�{���A�Yzk�̪��ƾڸ`�I�o�ͧ��ܡA�N�|�I�s�o��method
			 * �ڭ̧Ʊ�b�o��zk�����o�ͧ��ܮɡA�n�d�ߦb�u���A�Ⱦ������ǡA�ҥH�n�b�o�Ӧ^�ը��call getOnlineServers()
			 * �ӷ�I�s�W�z��method��A�N�S�|�A���U��ť�@��(�]��zk.getChildren()�����Y)�A�N�o�ˤ��д`���A�F������ť������
			 */
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeChildrenChanged) {
					try {
						// �ƥ�^���޿褤�A�b���d��zk�W���A�u�A�Ⱦ��`�I�Y�i�A�d���޿褤�S�A�����U�F�l�`�I�ܤƨƥ��ť
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

	// �d�ߦb�u�A�Ⱦ��C��
	public void getOnlineServers() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren("/servers", true); // ���F�o�Ӥ�k�A�|���ͤ@��thread�t�d��ťzk���ܤ�
		ArrayList<String> servers = new ArrayList<String>();

		for (String child : children) {
			byte[] data = zk.getData("/servers/" + child, false, null);
			String hostInfo = new String(data);
			servers.add(hostInfo);
		}

		onlineServers = servers; // �bfor�~���A��ȵ�onlineServers�A�H�קK���ƪ��ȥ[�ionlineServers
		System.out.println("�d�ߤF�@��zk�A��e�b�u���A�Ⱦ���: " + servers);
	}

	// �B�z�~��(�V�@�x�A�Ⱦ��o�e�ɶ��d�߽ШD)
	public void sendRequest() throws InterruptedException, UnknownHostException, IOException {
		// �o���Y�O�~���޿�A�����ϥ�thread.sleep()�A�קKzk���ͪ���ťthread�H�ۥD�{�Ǫ��`�Ƥ]����
		// System.out.println("���O�̶}�l�B�z�~�ȥ\��...");
		// Thread.sleep(Long.MAX_VALUE);

		Random random = new Random();

		while (true) {
			try {
				// �D��@�x��e�A�u���A�Ⱦ�
				int nextInt = random.nextInt(onlineServers.size());
				String server = onlineServers.get(nextInt); //onlineServers��s(�ե�getOnlineServers())�ɥi��|������bget�ɴN���i������A�ҥH�[try-catch
				String hostname = server.split(":")[0];
				int port = Integer.parseInt(server.split(":")[1]);

				Socket socket = new Socket(hostname, port); // �V�A�Ⱦ��n�o�Xrequest���ܡA�ϥ�socket
				OutputStream out = socket.getOutputStream(); // �Ȥ�ݵo�X���ШD
				InputStream in = socket.getInputStream(); // �Ȥ�ݦ��쪺�ШD

				out.write("Hello".getBytes());
				out.flush();

				byte[] buf = new byte[256];
				int read = in.read(buf);
				System.out.println("�A�Ⱦ��T�����ɶ���:" + new String(buf, 0, read));

				out.close();
				in.close();
				socket.close();

				Thread.sleep(2000); // �C���d�@��
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		TimeQueryConsumer consumer = new TimeQueryConsumer();

		// �c�yzk�s����H
		consumer.connectZooKeeper();
		// �d�ߦb�u�A�Ⱦ��C��
		consumer.getOnlineServers();
		// �B�z�~��(�V�@�x�A�Ⱦ��o�e�ɶ��d�߽ШD)
		consumer.sendRequest();
	}
}
