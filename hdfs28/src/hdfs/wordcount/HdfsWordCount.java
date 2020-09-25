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
		 * ��l�Ƥu�@
		 */
		// �o�ؼg�k���n�B(�z�L�t�m���)�G
		// ���o�t�m���
		Properties props = new Properties();
//		props.load(new FileInputStream("job.properties"));
		props.load(HdfsWordCount.class.getClassLoader().getResourceAsStream("job.properties")); // �ϥ����[�������o��e���A�B��e������Ӱt�m���
		Path input = new Path(props.getProperty("INPUT_PATH")); // new�@��Path���|�A���٤��|�bHDFS���إ߸Ӹ��|�A�]���٨S�Mfs�[���A�b78�檺fs.create()�ɤ~�|�bHDFS���إ߸Ӹ��|
		Path output = new Path(props.getProperty("OUTPUT_PATH"));
		Class<?> mapper_class = Class.forName(props.getProperty("MAPPER_CLASS")); // Class.forName()�G��^�ѼƤ����w�����O�AClass.forName()�|�[��MAPPER_CLASS���w�����A�ëO�s��mapper_class
		Mapper mapper = (Mapper)mapper_class.newInstance(); // newInstance����{���A�u�O�j�নMapper
		Context context = new Context();
//		WordCountMapper wordCountMapper = new WordCountMapper();
		
		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), new Configuration(), "root");
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(input, false);

		
		/**
		 * �B�z�ƾ�
		 */
		// 1. �hHDFS��Ū�����G�@��Ū�@��
		while(iter.hasNext()) {
			LocatedFileStatus file = iter.next();
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(in)); // ���奻���ɡA�i�H�ϥ�BufferedReader
			String line = null;
			
			// 3. �N�o�@�檺�B�z���G��J�@�ӽw�s
			while((line = br.readLine()) != null) {
				// 2. �եΤ@�Ӥ�k��C�@��i��~�ȳB�z
				mapper.map(line, context);
//				wordCountMapper.map(line, context);
			}
			
			br.close();
			in.close();
		}
		
		/**
		 * ��X���G
		 */
		// 4. �եΤ@�Ӥ�k�N�w�s�������G�ƾڿ�X��HDFS���G���
		HashMap<Object, Object> contextMap = context.getContextMap();
//		Path outPath = new Path("/wordcount/output");
		
		if(fs.exists(output)) { 
			throw new RuntimeException("���w����X�ؿ��w�s�b�A�Ч�...");
		}
		
		FSDataOutputStream out = fs.create(new Path(output, new Path("res.dat")));
		
		Set<Entry<Object, Object>> entrySet = contextMap.entrySet(); // return set of the mappings
		
		for (Entry<Object, Object> entry : entrySet) { // entry�O�@��key-value pair
			out.write((entry.getKey() + "\t" + entry.getValue() + "\n").getBytes());
		}
		
		out.close();
		fs.close();
		
		System.out.println("�ƾڲέp����...");
	}
}
