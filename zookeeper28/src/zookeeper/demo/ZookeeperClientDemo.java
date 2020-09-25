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
		// �Ыؤ@�ӳs��zookeeper���Ȥ�ݹ�H
		// 1. �s�����Ұ�zookeeper�������A�q�{port�O2181
		// 2. sessionTimeout: �Y�W�Xtimeout�ȡA���}session�A���A����zookeeper��ƾڸ`�I���B�z
		// 3. watcher: zookeeper�t�d��ť�A����q����A�i�H�ΦW��{�@�ӫȤ�ݹ���ܪ��ƾڸ`�I�һݰ����~���޿誺���f�Anull�N��S����ť���ݨD�A�O�@�ӵ�f����{��
		//    �i�H�gnull�A�N����q���ᤣ�n�������
		// (Note): �D���������r�����ᤣ�i���Ů�A���M��ѪR��[�Ů�]hdp-02:2181
//		ZooKeeper zooKeeper = new ZooKeeper("hdp-01:2181,hdp-02:2181,hdp-03:2181", 2000, null);
		
		// �Ыؼƾڸ`�I
		// 3. ACL(access control list): �X���v���A�����������Ȥ�i�H�X�ݸӼƾڸ`�I�A���L�o�Ӽƾڸ`�I�b�o�ӮרҤ�
		//    �O���t�ΨϥΪ��A���O�Y�ӯS�w���Ȥ�(�H)�A�ҥH���@�Ӷ}���v���N�n
		// 4. createMode: �ƾڸ`�I������
		// ��^�ȬO�ҫت��ƾڸ`�I���|: /eclipse
		String create = zooKeeper.create("/eclipse", "HelloEclipse".getBytes(), 
				Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		System.out.println(create);
		
		zooKeeper.close();
	}
	
	@Test
	public void testUpdate() throws UnsupportedEncodingException, KeeperException, InterruptedException {
		// 3. version: ���P�����W��zookeeper�i��������P��������ƾڸ`�I�A�i�H���w�n�ק���Ӫ���
		//    -1�N����version�A���n�ק�
		zooKeeper.setData("/eclipse", "Tracy Mcgrady".getBytes("UTF-8"), -1);
		zooKeeper.close();
	}
	
	@Test
	public void testGet() throws KeeperException, InterruptedException, UnsupportedEncodingException {
		// 2. watch: �O�_�n��ť
		// 3. stat: �nget���Ӫ����Anull�N��̷s����
		byte[] data = zooKeeper.getData("/eclipse", false, null);
		System.out.println(new String(data, "UTF-8"));
		zooKeeper.close();
	}
	
	@Test
	public void testGetListChild() throws KeeperException, InterruptedException {
		// 1. �`�I���| 2. �O�_�n��ť
		// �`�N�G��^�����G���u���l�`�I�W�r�A���a�����|
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
