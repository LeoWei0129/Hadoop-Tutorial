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
 * �A�Ⱥݪ��D�{��
 * @author Leo
 * @version 1.0
 * @date 2019�~10��23�� �U��11:08:29
 * @remarks TODO
 *
 */

public class TimeQueryServer {
	ZooKeeper zk = null;
	
	// �c�yzk�Ȥ�ݳs��
	public void connectZooKeeper() throws IOException {
		zk = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				
			}});
	}
	
	// ���U�A�Ⱦ��H��
	public void registerServerInfo(String hostname, String port) throws Exception {
		// ���P�_���U�`�I���_�`�I�O�_�s�b�A�p�G���s�b�A�h�Ы�
		// �p�G�b�٨S�Ыؤ��`�I�����p�U�N�����Ыؤl�`�I�A�|����
		// 2. �O�_�n��ť�o�Ӽƾڸ`�I�A�ثe���ݭn�A�u�n�P�_�L�O�_�s�b�N�n
		// ��^�Ӽƾڸ`�I�����ƾڪ��A�T���A�����OStat
		Stat stat = zk.exists("/servers", false);
		if(stat == null) {
			zk.create("/servers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		// ���U�A�Ⱦ��ƾڨ�zk�����w���U�`�I�U
		String create = zk.create("/servers/server", (hostname+":"+port).getBytes(), 
				Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(hostname + " �A�Ⱦ��Vzk���U�H�����\�A���U���`�I��: " + create);
		
//		zk.close(); // ���nclose�Aclose���ܡA����U�W�h���`�I�S�|�Q�R��
	}
	
	public static void main(String[] args) throws Exception {
		TimeQueryServer timeQueryServer = new TimeQueryServer();
		
		// �c�yzk�Ȥ�ݳs��
		timeQueryServer.connectZooKeeper();
		
		// ���U�A�Ⱦ��H��
		// �ܧ�hostname��port�A�N�i�H�A�Ыإt�@�x�A�Ⱥݾ����Pzk�s��
		timeQueryServer.registerServerInfo(args[0], args[1]);
		
		// �Ұʷ~�Ƚu�{�}�l�B�z�~��
		// ����zk�bcreate�ƾڸ`�I��|�Ұʪ��u�{�A�z�Lport�s��zk�ᦹthread
		// ���ݭn�b�o��Thread.sleep()�A �]���s���ͪ�thread�|�bwhile loop(true)������P�Ȥ�ݫO���pô
		// ���|�h�X�l�u�{�A�ҥH�ثe���o�ӥD�u�{main()�]�|�������line 70�A���|�h�X�A�Ыت�zk�]���|����
		new TimeQueryService(Integer.parseInt(args[1])).start();
	}
}
