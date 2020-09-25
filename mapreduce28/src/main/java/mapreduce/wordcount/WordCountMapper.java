package mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN: �Omap taskŪ���쪺�ƾڪ�key�������A�O�@�檺�_�l�����qLong
 * VALUEIN: �Omap taskŪ���쪺�ƾڪ�value�������A�O�@�檺���eString
 * KEYOUT: �O�Τ᪺�۩w�qmap��k�n��^�����G(key-value)�ƾڪ�key�������A�bwordcount�޿褤�A�ݭn��^���O���String
 * VALUEOUT: �O�Τ᪺�۩w�qmap��k�n��^�����G(key-value)�ƾڪ�vlaue�����A�bwordcount�޿褤�A�ڭ̻ݭn��^���O���Integer
 * 
 * ���O�A�bmapreduce���Amap���ͪ��ƾڻݭn�ǿ鵹reduce�A�N�ݭn�i��ǦC�ƩM�ϧǦC�ơA��jdk������ͧǦC�ƾ���ͪ��ƾڶq������l�A
 * �N�|�ɭP�ƾڦbmapreduce�B��L�{���Ĳv�C�U�A�ҥH�Ahadoop�M���]�p�F�ۤv���ǦC�ƾ���A����Amapreduce���ǿ骺�ƾ������N������
 * �{hadoop�ۤv���ǦC�Ʊ��f
 * (EX): Long�u��{���jdk��Serialize���f�A�S����{hadoop���ǦC�Ʊ��f�ALong�]�N�L�k�Qhadoop���ǦC�ƦӨϥΡA�ҥH�ݭn�@�ӱM����hadoop�ǦC�ƪ��ƾ������A
 *       �o�O�z�Lhadoop�ʸ�Long�A�H��{hadoop�ǦC�Ʊ��f
 *  hadoop��jdk���`�ΰ�����Long�BString�BInteger�BFloat���ƾ������ʸ� �F�ۤv�ҹ�{��hadoop�ǦC�Ʊ��f����: LongWriteable�BText�BIntWritable�BFloatWritable  
 * 
 * 
 * @author Leo
 * @version 1.0
 * @date 2019�~9��28�� �W��11:07:14
 * @remarks TODO
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
//		super.map(key, value, context); �ثe���רҤ��n�h��{������map()
		// �����
		String line = value.toString(); // �Ӧ檺value�]�����O�OText�A�ҥH�n�নString�A�~�వsplit()
		String[] words = line.split(" ");
		
		for (String word : words) {
			// �]��Mapper���O����^�����OText�MIntWritable�A�ҥH�n���l�����@�]�˫�~write�icontext
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
