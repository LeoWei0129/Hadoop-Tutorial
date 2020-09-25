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
		 * Configuration參數對象的機制：
		 * 1. 構造時，會加載jar包中的默認配置xx-default.xml
		 * 2. 再加載用戶自己配置的xx-site.xml，覆蓋掉默認參數
		 * 3. 構造完成後，還可以conf.set("p", "v")，會再次覆蓋用戶配置文件中的參數值
		 */
		// 客戶端在構造時，可以指定hdfs的相關配置參數，可上網參考文檔(如下)
		// new Configuration()會從項目的classpath中加載core-default.xml、hdfs-default.xml、mapred-default.xml
		Configuration conf = new Configuration();
//		conf.set("fs.defautFS", "hdfs://hdp-01:9000"); // 此行可寫不可寫
		conf.set("dfs.replication", "2"); // 指定副本數: 2
		conf.set("dfs.blocksize", "64m"); // 指定塊大小: 64MB
		
		// 訪問客戶端，構造一個訪問指定HDFS系統的客戶端對象
		// 1. 指定程序訪問端口
		// 2. 用戶可指定hdfs的相關配置參數
		// 3. 客戶端的身分(用戶名)
		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000/"), conf, "root");
		
		// 上船一個文件到HDFS中，
		// 2. 放到HDFS系統中，不需指名hdfs系統(如上)，只需寫路徑(根目錄直接"/"表示)
		// HDFS的方法中，new Path()裡不須指定hdfs://hdp-01:9000/，如：hdfs://hdp-01:9000/test/
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
	 * 測試方法
	 * 從HDFS中下載文件到客戶端本地磁盤
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
	 * 在HDFS內部移動並修改文件名稱
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRename() throws IllegalArgumentException, IOException {
		fs.rename(new Path("/test/toeic.pdf"), new Path("/"));
		fs.close();
	}
	
	/**
	 * 在HDFS中創建文件夾
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testMkdirs() throws IllegalArgumentException, IOException {
		fs.mkdirs(new Path("/tempdir/subdir1/subdir2")); // 若有其中一個路徑不存在，HDFS會自動創建
		fs.close();
	}
	
	/**
	 * 在HDFS中刪除文件夾或文件
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRemove() throws IllegalArgumentException, IOException {
		fs.delete(new Path("/tempdir/subdir1/subdir2"), true);
		fs.close();
	}
	
	/**
	 * 查詢HDFS指定目錄下的文件信息
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testLookUp1() throws IllegalArgumentException, IOException {
		// listFiles()指定使用迭代器取每個文件的信息(文件名、路徑、塊大小...)，該方法不返回文件夾的信息
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true); // 列出指定路徑下的文件, 2. recursive: 是否要把指定目錄下的子文件夾的檔案也列出來
		
		while(iter.hasNext()) {
			LocatedFileStatus status = iter.next(); // next()取得一個文件描述對象，非文件本身
			System.out.println("文件全路徑: " +  status.getPath());
			System.out.println("塊大小: " + status.getBlockSize());
			System.out.println("文件長度: " + status.getLen());
			System.out.println("副本數量: " + status.getReplication());
			System.out.println("塊信息: " + Arrays.toString(status.getBlockLocations()));
			System.out.println("-----");
		}
		
		fs.close();
	}
	
	/**
	 * 查詢HDFS指定目錄下的文件及文件夾信息
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testLookUp2() throws IllegalArgumentException, IOException {
		// listStatus()返回一個數組
		FileStatus[] iter = fs.listStatus(new Path("/")); // 只返回指定目錄的文件以及當前子目錄的信息
		
		for(FileStatus status: iter) {
			System.out.println("文件全路徑: " +  status.getPath());
			System.out.println("是否是文件夾:" + (status.isDirectory()? "Yes": "No"));
			System.out.println("塊大小: " + status.getBlockSize());
			System.out.println("文件長度: " + status.getLen());
			System.out.println("副本數量: " + status.getReplication());
			System.out.println("-----");
		}
		
		fs.close();
	}
	
	/**
	 * 讀取HDFS中的文件內容
	 * Java中，讀文件時，以stream(流)的方式一個字節一個字節(byte)讀取
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testReadData() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fs.open(new Path("/wiki_english.txt")); // 數據流
		
		// 先以byte的形式讀取，再轉換程我們要的形式
//		byte[] buf = new byte[1024];
//		in.read(buf); // 文件讀到buf中
//		
//		System.out.println(new String(buf));
		// br都會從頭開始讀
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8")); // 字符流包裝數據流
		
		String line = null;
		while((line = br.readLine()) != null) {
			System.out.println(line);
		}
		
		br.close();
		in.close();
		fs.close();
	}
	
	/**
	 * 讀取HDFS中的指定偏移量範圍的文件內容
	 * Java中，讀文件時，以stream(流)的方式一個字節一個字節(byte)讀取
	 * 應用：實現讀取一個文本文件中的的指定BLOCK塊中的所有數據
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testRandomReadData() throws IllegalArgumentException, IOException {
		FSDataInputStream in = fs.open(new Path("/wiki_english.txt")); // 數據流(byte流)
		
		// 先以byte的形式讀取，再轉換程我們要的形式
//		byte[] buf = new byte[1024];
//		in.read(buf); // 文件讀到buf中
//		
//		System.out.println(new String(buf));
		
		in.seek(100); // 將讀取的起始位置進行指定(從第100個byte開始)
		
		byte[] buf = new byte[16]; // 指讀取文件其中的16個byte
		
		in.read(buf); // 文件讀到buf中
		System.out.println(new String(buf));
		
		in.close();
		fs.close();
	}
	
	/**
	 * 往HDFS中的文件寫內容
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testWriteData() throws IllegalArgumentException, IOException {
		// 寫文件使用create()，若文件不存在，則創建
		// 2: 是否覆蓋當前文件
		FSDataOutputStream out = fs.create(new Path("/write.jpg"), false);
		FileInputStream in = new FileInputStream("D:/Apache Ecosystem/bra.jpg"); // byte流
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
