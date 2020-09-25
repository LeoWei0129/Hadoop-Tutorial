package mapreduce.page;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 1. LongWritable: �C��_�l�����q
 * 2. Text: �C�@��
 * 3. Text: page������url
 * 4. IntWritable: page������
 * 3�M4�n�ǿ鵹reduce task��key-value��
 * ��J�ɮת��C��榡����: ���        page��url
 * @author Leo
 * @version 1.0
 * @date 2019�~10��9�� �U��11:03:49
 * @remarks TODO
 *
 */

public class PageTopnMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split(" ");
		context.write(new Text(split[1]), new IntWritable(1)); // key-value pair
	}
}
