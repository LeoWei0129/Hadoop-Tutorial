package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * �Ω󴣥�mapreduce job���Ȥ�ݵ{�Ǩ�Linux����Yarn���s�W�B��(�⥴�]�n��jar�]���浹Yarn)
 * �]�N�O���A�bEclipse����JobSubmiiter��main()�A�q�L��Job(�]�tmap task�Mreduce task)���t�m�A��jar�]�浹Yarn���s
 * Yarn���s�N�|��jar�]�̪�Mapper�MReducer�ӹB��(�Ӥ��O�g�bEclipse����Mapper�MReducer)
 * 
 * �qEclipse�W����main()�A�H�Njar�]���浹Yarn(�I��|�ϥ�java -cp���O)�AYarn�|�۰ʽեΩҨϥΨ쪺�̿�]�A�o�M�qLinux�W����job�Ȥ�ݵ{�Ǥ��P
 * �Y�n�bLinux�W�B��JobSubmitter�A�Өϥ�java -cp�ӹB�檺�ܡA�����@�Ӥ@�Ӽg�ӵ{�ǩҭn�ϥΨ쪺�̿�]�A�o�|�ܳ·�
 * �ҥH�n�ϥ�Hadoop jar ..jar JobSubmitter�Ӱ���(�Բӻ����bJobSubmitterLinuxYarn)
 * 
 * �\��G
 * 1. �ʸ˥���job�B��ɩһݭn�����n�Ѽ�
 * 2. ��yarn�i��椬�A�Nmapreduce�{�Ǧ��\���ҰʡB�B��
 * @author Leo
 * @version 1.0
 * @date 2019�~9��29�� �W��1:32:14
 * @remarks TODO
 *
 */

public class JobSubmitterWindowsYarn {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		// �b�N�X���]�mJVM�t�ΰѼơA�Ω�job��H�H����X��HDFS���Τᨭ��
		System.setProperty("HADOOP_USER_NAME", "root");
		
		// 1. �]�mjob�B��ɭn�X�ݪ��q�{���t�ΡA���]�m�o�Ӫ��ܡAhadoop�|�h��core-default.xml�A�Ӥ��̰t�m��fs.defaultFS�q�{��file://
		conf.set("fs.defaultFS", "hdfs://hdp-01:9000");
		// 2. �]�mjob�������h�B��(�H�ƻ�Ҧ��B��Glocal��yarn)�A�o�̧�job�����yarn���s���B��
		conf.set("mapreduce.framework.name", "yarn");
		// 3. �]�mresourcemanager
		conf.set("yarn.resourcemanager.hostname", "hdp-01");
		// 4. �p�G�n�qwindows�t�ΤW�B��o��job����Ȥ�ݵ{�Ǩ�Linux�W��yarn���s�h����A�h�ݭn�]�m�o�Ӹ󥭥x���檺�ѼƬ�true
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		Job job = Job.getInstance(conf);

		// 1. �ʸ˰ѼơGjar�]�Ҧb����m
		job.setJar("D:/Apache Ecosystem/myjars/wordcount.jar");
//		job.setJarByClass(JobSubmitter.class); // ���o����(�i�H�۰���������Ҧb���|�A�]�N�O�i�H�z�L�������o��e��Jar�Ҧb���|�A�Ǧ����oJar�])�AsetJarByClass()�i�H�ʺA���o���Ҧb��m
		
		// 2. �ʸ˰ѼơG����job�ҭn�եΪ�Mapper��{���BReduce��{��
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		// 3. �ʸ˰ѼơG����job��Mapper��{���BReducer��{�����ͪ����G�ƾڪ�key�Bvalue����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
//		Path output_maven = new Path("hdfs://hdp-01:9000/wordcount/output_maven");
//		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-01:9000"), conf, "root");
//		if(fs.exists(output_maven)) {
//			fs.delete(output_maven, true);
//		}
		
		// 4. �ʸ˰ѼơG����job�n�B�z����J�ƾڤΩҦb���|�B�̲׵��G����X���|
		// �u�n�bhadoop�U��configuration���T�t�m���t�Φp: hdfs://hdp-01:9000/
		// HDFS�BMapReduce�BYarn���|�ϥγo�ӨϥΤ��t�ΡA������wPath�u�ݦA�g�H�U�����|�Y�i
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output_maven")); // ��X���|�������s�b
		
		// 5. �ʸ˰ѼơG�Q�n�Ұʪ�reduce task���ƶq
		job.setNumReduceTasks(2); // �q�{�@��
		
		// 6. ����job��yarn
		// job.waitForCompletion()�G����mapreduce�����A�|�@����resource manager�O���pô�A�B
		// resource manager�|�V�Ȥ�ݧ�@�Ƕi�װT���i����X
		// �ѼơG�O�_�n��resource manager��^���ѼƦL�X��
		// ��^�ȡG�o��mapreduce�{�Ǫ��B�浲�G�O�_���\
		boolean result = job.waitForCompletion(true);
		
		// �i��Ұʸ}������@�Ӧ۩w����^�ȡG�H�P�_mapreduce�O�_�B�榨�\�A�]��main��k����^�Ȫ����Y
		System.exit(result? 0: -1);
	}
}












