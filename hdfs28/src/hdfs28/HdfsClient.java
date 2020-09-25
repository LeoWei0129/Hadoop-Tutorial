package hdfs28;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient {
	FileSystem fs = null;
	
	public static void main(String[] args) throws Exception {
		/**
		 * Configuration�Ѽƹ�H������G
		 * 1. �c�y�ɡA�|�[��jar�]�����q�{�t�mxx-default.xml
		 * 2. �A�[���Τ�ۤv�t�m��xx-site.xml�A�л\���q�{�Ѽ�
		 * 3. �c�y������A�٥i�Hconf.set("p", "v")�A�|�A���л\�Τ�t�m��󤤪��Ѽƭ�
		 */
		// �Ȥ�ݦb�c�y�ɡA�i�H���whdfs�������t�m�ѼơA�i�W���ѦҤ���(�p�U)
		// new Configuration()�|�q���ت�classpath���[��core-default.xml�Bhdfs-default.xml�Bmapred-default.xml
		Configuration conf = new Configuration();
//		conf.set("fs.defautFS", "hdfs://hdp-01:9000"); // ����i�g���i�g
		conf.set("dfs.replication", "2"); // ���w�ƥ���: 2
		conf.set("dfs.blocksize", "64m"); // ���w���j�p: 64MB
		
		// �X�ݫȤ�ݡA�c�y�@�ӳX�ݫ��wHDFS�t�Ϊ��Ȥ�ݹ�H
		// 1. ���w�{�ǳX�ݺݤf
		// 2. �Τ�i���whdfs�������t�m�Ѽ�
		// 3. �Ȥ�ݪ�����(�Τ�W)
		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000/"), conf, "root");
		
		// �W��@�Ӥ���HDFS���A
		// 2. ���HDFS�t�Τ��A���ݫ��Whdfs�t��(�p�W)�A�u�ݼg���|(�ڥؿ�����"/"���)
		// HDFS����k���Anew Path()�̤������whdfs://hdp-01:9000/�A�p�Ghdfs://hdp-01:9000/test/
		fs.copyFromLocalFile(new Path("D:/Apache Ecosystem/test.csv.zip"), new Path("/test/"));
		fs.close();
	}
	
	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "4"); 
		conf.set("dfs.blocksize", "64m"); 
		
		fs = FileSystem.get(new URI("hdfs://hdp-01:9000/"), conf, "root");
	}
	
	/**
	 * ���դ�k
	 * �qHDFS���U������Ȥ�ݥ��a�ϽL
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test 
	public void testGet() throws IllegalArgumentException, IOException {
//		fs.copyToLocalFile(new Path("/install.log"), new Path("D:/Apache Ecosystem/test/"));
		fs.copyFromLocalFile(new Path("D:/Apache Ecosystem/girl.jpg"), new Path("/tempdir/subdir1/subdir2/"));
		fs.close();
	}
	
	/**
	 * �bHDFS�������ʨíק���W��
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRename() throws IllegalArgumentException, IOException {
		fs.rename(new Path("/test/toeic.pdf"), new Path("/"));
		fs.close();
	}
	
	/**
	 * �bHDFS���Ыؤ��
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testMkdirs() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/tempdir/subdir1/subdir2")); // �Y���䤤�@�Ӹ��|���s�b�AHDFS�|�۰ʳЫ�
		fs.close();
	}
	
	/**
	 * �bHDFS���R����󧨩Τ��
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRemove() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/tempdir/subdir1/subdir2"), true);
		fs.close();
	}
	
	/**
	 * �d��HDFS���w�ؿ��U�����H��
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testLookUp1() throws IllegalArgumentException, IOException {
		// listFiles()���w�ϥέ��N�����C�Ӥ�󪺫H��(���W�B���|�B���j�p...)�A�Ӥ�k����^��󧨪��H��
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true); // �C�X���w���|�U�����, 2. recursive: �O�_�n����w�ؿ��U���l��󧨪��ɮפ]�C�X��
		
		while(iter.hasNext()) {
			LocatedFileStatus status = iter.next(); // next()���o�@�Ӥ��y�z��H�A�D��󥻨�
			System.out.println("�������|: " +  status.getPath());
			System.out.println("���j�p: " + status.getBlockSize());
			System.out.println("������: " + status.getLen());
			System.out.println("�ƥ��ƶq: " + status.getReplication());
			System.out.println("���H��: " + Arrays.toString(status.getBlockLocations()));
			System.out.println("-----");
		}
		
		fs.close();
	}
	
	/**
	 * �d��HDFS���w�ؿ��U�����Τ�󧨫H��
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testLookUp2() throws IllegalArgumentException, IOException {
		// listStatus()��^�@�ӼƲ�
		FileStatus[] iter = fs.listStatus(new Path("/")); // �u��^���w�ؿ������H�η�e�l�ؿ����H��
		
		for(FileStatus status: iter) {
			System.out.println("�������|: " +  status.getPath());
			System.out.println("�O�_�O���:" + (status.isDirectory()? "Yes": "No"));
			System.out.println("���j�p: " + status.getBlockSize());
			System.out.println("������: " + status.getLen());
			System.out.println("�ƥ��ƶq: " + status.getReplication());
			System.out.println("-----");
		}
		
		fs.close();
	}
	
	/**
	 * Ū��HDFS������󤺮e
	 * Java���AŪ���ɡA�Hstream(�y)���覡�@�Ӧr�`�@�Ӧr�`(byte)Ū��
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testReadData() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fs.open(new Path("/wiki_english.txt")); // �ƾڬy
		
		// ���Hbyte���Φ�Ū���A�A�ഫ�{�ڭ̭n���Φ�
//		byte[] buf = new byte[1024];
//		in.read(buf); // ���Ū��buf��
//		
//		System.out.println(new String(buf));
		// br���|�q�Y�}�lŪ
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8")); // �r�Ŭy�]�˼ƾڬy
		
		String line = null;
		while((line = br.readLine()) != null) {
			System.out.println(line);
		}
		
		br.close();
		in.close();
		fs.close();
	}
	
	/**
	 * Ū��HDFS�������w�����q�d�򪺤�󤺮e
	 * Java���AŪ���ɡA�Hstream(�y)���覡�@�Ӧr�`�@�Ӧr�`(byte)Ū��
	 * ���ΡG��{Ū���@�Ӥ奻��󤤪������wBLOCK�������Ҧ��ƾ�
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testRandomReadData() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fs.open(new Path("/wiki_english.txt")); // �ƾڬy(byte�y)
		
		// ���Hbyte���Φ�Ū���A�A�ഫ�{�ڭ̭n���Φ�
//		byte[] buf = new byte[1024];
//		in.read(buf); // ���Ū��buf��
//		
//		System.out.println(new String(buf));
		
		in.seek(100); // �NŪ�����_�l��m�i����w(�q��100��byte�}�l)
		
		byte[] buf = new byte[16]; // ��Ū�����䤤��16��byte
		
		in.read(buf); // ���Ū��buf��
		System.out.println(new String(buf));
		
		in.close();
		fs.close();
	}
	
	/**
	 * ��HDFS�������g���e
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testWriteData() throws IllegalArgumentException, IOException {
		// �g���ϥ�create()�A�Y��󤣦s�b�A�h�Ы�
		// 2: �O�_�л\��e���
		FSDataOutputStream out = fs.create(new Path("/write.jpg"), false);
		FileInputStream in = new FileInputStream("D:/Apache Ecosystem/bra.jpg"); // byte�y
		byte[] buf = new byte[1024];
		int read = 0;
		
		while((read = in.read(buf)) != -1) {
			out.write(buf, 0, read);
		}
		
		in.close();
		out.close();
		fs.close();
	}
}
