package mapreduce.flow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * �����|����Mapper�ǿ�X�h��key-value�A�ë��Ӧ����g���޿�M�w�C��group�n���o�����x�����W��Reducer
 * �ҥHPartitioner����key-value��������Mapper�ǿ�X�hkey-value����
 * ���okey-value��Reducer���O�o�����������A�O��Map task������
 * Map task�b�����ۤv�����ȫ�A�|�եγo����(�Qnew)
 * �����O���ѵ�MapTask�Ϊ�
 * MapTask�q�L�o������getPartition��k�A�ӭp�⥦�Ҳ��ͪ��C�@��kv�ƾڸӤ��o�����@��reduce task
 * @author Leo
 * @version 1.0
 * @date 2019�~10��15�� �U��9:11:44
 * @remarks TODO
 *
 */
public class ChangePartitioner extends Partitioner<Text, FlowBean> {
	// �q��Ʈw�եΤ�����X�������ϽX�A�o�̨B��{�A�����g��
	// ������X���e�T�X�O�e��A�ĥ|�X�O�ϽX
	// �C��Map task�b�եγo�����ɡA���|�֦��@�P�@��codeMap�A�o�˥u�ݭn�X�ݸ�Ʈw�@��
	// String: ���X�e�Y
	// Integer: �ϽX
	static HashMap<String, Integer> codeMap = new HashMap<String, Integer>();
	
	// �bnew�o�����ɡA�|�[���o��static
	static {
		codeMap.put("091", 0);
		codeMap.put("092", 1);
		codeMap.put("093", 2);
		codeMap.put("094", 3);
		codeMap.put("095", 4);
	}
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		Integer code = codeMap.get(key.toString().substring(0, 3));
		return code == null? 5: code; // 5�N��Y�䤣��ϽX�A�|����6��reducer�Ӱ���o��key-value
	}

}
