package mapreduce.flow;

import java.util.Map.Entry;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;

/**
 * 
 * TreeMap�i�H��򥻼ƾ��������ƧǡA�]�i�H�����O���O���ƧǡA���ݭn�ۤv�B�~��{compare()��k�A�H�M�w�ƧǭӳW�h����
 * @author Leo
 * @version 1.0
 * @date 2019�~10��8�� �W��1:36:01
 * @remarks TODO
 *
 */

public class TreeMapTest {
	public static void main(String[] args) {
		// �ѩ󤣬Ointerface��abstract�A�H�U�e4��statement���i�H�z�L�ΦW���覡�ӽե�
//		new JobSubmitter();
//		new FlowBean();
//		new FlowSumMapper();
//		new FlowSumReducer();
//		new Comparator<FlowBean>(); -> ����
		
//		TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>();
//		
//		treeMap.put("a", 2);
//		treeMap.put("ab", 5);
//		treeMap.put("aa", 10);
//		treeMap.put("b", 1);
		
		// ���]�{�b�n�ھ��`�y�q���Ƨ�(�W��y�qupFlow+�U��y�qdownFlow): FlowBean�@��key
		// TreeMap���@�ӱaComparator������Ѽƪ�constructor�A�Ω�i�H�ۭq�q������W�h�M���ǡA�q�`�Ω�key�O���O��
		// �]�����O�q�`�������������@�Ӥ�����̾�
		// �ϥΰΦW�ŧi���覡�ӹ�@����Comparator��compare()��k�A�H�w�q������W�h
		// ��: ������ܦanew�����A���i�H�ΦW�ŧi���覡�ӹ갵����
		// int compare()�O����Comparator�����Q��@����k
		// �Y�J�������j�p�ۦP�ɡATreeMap�|�ϥΥ�put()�i�h���ȷ�@�̲׭ȡA�t�~�@�ӭȷ|�Q�л\��
		// �ҥH�o�̦A�i�@�B�P�_�A�ϥΦr��q�ܸ��X���j�p�ӧP�_�j�p
		TreeMap<FlowBean, String> treeMap = new TreeMap<FlowBean, String>(new Comparator<FlowBean>() {

			public int compare(FlowBean o1, FlowBean o2) {
				if(o2.getTotalFlow() - o1.getTotalFlow() == 0) {
					return o1.getPhone().compareTo(o2.getPhone());
				}
				return o2.getTotalFlow() - o1.getTotalFlow();
			}
		});
		
		FlowBean bean1 = new FlowBean(500, 200, "0954123856");
		FlowBean bean2 = new FlowBean(100, 250, "0954128452");
		FlowBean bean3 = new FlowBean(800, 450, "0954120873");
		FlowBean bean4 = new FlowBean(550, 300, "0954126201");
		FlowBean bean5 = new FlowBean(400, 300, "0954120124");
		
		treeMap.put(bean1, null);
		treeMap.put(bean2, null);
		treeMap.put(bean3, null);
		treeMap.put(bean4, null);
		treeMap.put(bean5, null);
		
		// Returns a Set view of the mappings contained in this map. 
		// The set's iterator returns the entries in "ascending key order".
		// An entry: (key, value) pair
		Set<Entry<FlowBean, String>> entrySet = treeMap.entrySet();
		
		// �Y�����N�Ыث᪺FlowBean���treeMap���A�å�foreach�ӱƧǡA�ѩ��FlowBean��@key�A��FlowBean��
		// �S���w�q������W�h�AtreeMap�|�����D�p��ƧǡA�|����
		for (Entry<FlowBean, String> entry : entrySet) {
			System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
		}
	}
}
