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
		// �Ыؤ@�ӳs��zookeeper���Ȥ�ݹ�H
		// �Ȥ�ݹ�H�^�ծɪ�watcher�Awatcher�N���μg�bgetData()�̡A�������true���o��method�N�n
		zooKeeper = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// �קK�b��l�Ƹ`�I�ɤ]�եΤF�D�n���~���޿�(�ڭ̥u�n�ƾڸ`�I�o�ͧ��ܮɤ~�եγo�Ӥ�k)
				if(event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeDataChanged) {
					System.out.println(event.getPath()); // ���쪺�ƥ�ҵo�ͪ��ƾڸ`�I���|
					System.out.println(event.getType()); // ���쪺�ƥ�����
					System.out.println("����k�O��@����q����ҭn�B�z���~���޿�"); // ����ƥ��A�ڭ̪��B�z�޿�
				
					// ���U�@����ť�A��ƾڸ`�I�o�ͧ��ܡA��e���o�Ӻ�ť�N�|�Q���P�A�N�����A���U�@����ť
					// �U�����g�k�i�H�T�O���к�ť
					// �쥻�OzooKeeper���U��ť�A����q����|�ե�process()��k�A�S�|�b�o�̵��U��ť�@���A�ҥH�i�H�F����к�ť
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
		// �}�Һ�ť�\��
		// zooKeeper���槹getData()��A�|�A�Ұʤ@�ӽu�{�t�d��ť
		// Thread.sleep()�t�d�����o��method�����A���MzooKeeper�]�|��۳Q�^���A�N���|�A��ť�F
		// 2: �i�H�Otrue�A�N��n��ť�A�άO�@�ӱ��f����{watcher�A�N����ƾڸ`�I�����ܪ��q����A�n�����B�z
		// new Watcher()�Ozookeeper������ܳq���᪺�@�Ӧ^���޿�(rollback)�A�ЫثȤ�ݮɴN�gwatcher
		// �b�C�����γo�Ӥ�k�ɡA�]���|�ե�watcher(�b�����ܮ�)
		byte[] data = zooKeeper.getData("/eclipse", true, null); // ��ť�`�I�ƾ��ܤ�
//		byte[] childData = zooKeeper.getData("/aa/aab", true, null); // ��ť�`�I���l�`�I�ܤƨƥ�
		System.out.println(new String(data, "UTF-8"));
		Thread.sleep(Long.MAX_VALUE); // �ثe�o�ӽu�{(��k)�Asleep�A��zookeeper�S�Ыت��t�@�ӽu�{�����ť
//		zooKeeper.close(); // �{�bzooKeeper�n�t�d��ť�A�ҥH���i�Hclose
	}
}
