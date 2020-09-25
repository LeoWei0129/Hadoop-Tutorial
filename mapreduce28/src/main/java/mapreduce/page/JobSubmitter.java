package mapreduce.page;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobSubmitter {
	
	public static void main(String[] args) throws Exception{
		// Windows Local狠笲︽碞ぃノ砞竚把计
		// 1. 硄筁更classpath*-site.xmlゅン秆猂把计
		// ぃconf.addResource()hdfs穦笆更core-site.xml㎝hdfs-site.xmlㄤぃ粄眔
		// 癸ㄤ*-site.xml惠璶硓筁con.addResource()タ絋更
		Configuration conf = new Configuration();
		conf.addResource("*-site.xml");
		
		// 2. 硄筁絏砞竚把计
//		conf.setInt("top.n", 3); // 砞﹚璹把计㎝
		
		// 3. 硄筁妮┦皌竚ゅン莉把计(皌竚topn.propertiesゅン)把σ玡ㄒ
		
		// conf砆杆Job柑ぇ碞眖Job眔闽把计(ㄒ:眖context眔)
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobSubmitter.class);
		
		job.setMapperClass(PageTopnMapper.class);
		job.setReducerClass(PageTopnReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/Apache Ecosystem/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/Apache Ecosystem/flow/output"));
		
		job.waitForCompletion(true);
	}

}
