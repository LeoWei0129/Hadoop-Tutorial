package hdfs.wordcount;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.conf.Configuration;

public class HdfsWordCount {
	public static void main(String[] args) throws Exception {
		/**
		 * 初始化工作
		 */
		// 這種寫法的好處(透過配置文件)：
		// 取得配置文件
		Properties props = new Properties();
//		props.load(new FileInputStream("job.properties"));
		props.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties")); // 使用類加載器取得當前類，且當前類獲取該配置文件
		Path input = new Path(props.getProperty("INPUT_PATH")); // new一個Path路徑，但還不會在HDFS中建立該路徑，因為還沒和fs架接，在78行的fs.create()時才會在HDFS中建立該路徑
		Path output = new Path(props.getProperty("OUTPUT_PATH"));
		Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS")); // Class.forName()：返回參數中指定的類別，Class.forName()會加載MAPPER_CLASS指定的類，並保存於mapper_class
		Mapper mapper = (Mapper)mapper_class.newInstance(); // newInstance為實現類，只是強轉成Mapper
		Context context = new Context();
//		WordCountMapper wordCountMapper = new WordCountMapper();
		
		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), new Configuration(), "root");
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(input, false);

		
		/**
		 * 處理數據
		 */
		// 1. 去HDFS中讀取文件：一次讀一行
		while(iter.hasNext()) {
			LocatedFileStatus file = iter.next();
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(in)); // 為文本文件時，可以使用BufferedReader
			String line = null;
			
			// 3. 將這一行的處理結果放入一個緩存
			while((line = br.readLine()) != null) {
				// 2. 調用一個方法對每一行進行業務處理
				mapper.map(line, context);
//				wordCountMapper.map(line, context);
			}
			
			br.close();
			in.close();
		}
		
		/**
		 * 輸出結果
		 */
		// 4. 調用一個方法將緩存中的結果數據輸出到HDFS結果文件
		HashMap<Object, Object> contextMap = context.getContextMap();
//		Path outPath = new Path("/wordcount/output");
		
		if(fs.exists(output)) { 
			throw new RuntimeException("指定的輸出目錄已存在，請更換...");
		}
		
		FSDataOutputStream out = fs.create(new Path(output, new Path("res.dat")));
		
		Set<Entry<Object, Object>> entrySet = contextMap.entrySet(); // return set of the mappings
		
		for (Entry<Object, Object> entry : entrySet) { // entry是一個key-value pair
			out.write((entry.getKey() + "\t" + entry.getValue() + "\n").getBytes());
		}
		
		out.close();
		fs.close();
		
		System.out.println("數據統計完成...");
	}
}
