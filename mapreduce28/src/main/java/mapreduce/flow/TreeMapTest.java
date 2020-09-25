package mapreduce.flow;

import java.util.Map.Entry;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeMap;

/**
 * 
 * TreeMap可以對基本數據類型做排序，也可以對類別型別做排序，但需要自己額外實現compare()方法，以決定排序個規則順序
 * @author Leo
 * @version 1.0
 * @date 2019年10月8日 上午1:36:01
 * @remarks TODO
 *
 */

public class TreeMapTest {
	public static void main(String[] args) {
		// 由於不是interface或abstract，以下前4個statement都可以透過匿名的方式來調用
//		new JobSubmitter();
//		new FlowBean();
//		new FlowSumMapper();
//		new FlowSumReducer();
//		new Comparator<FlowBean>(); -> 報錯
		
//		TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>();
//		
//		treeMap.put("a", 2);
//		treeMap.put("ab", 5);
//		treeMap.put("aa", 10);
//		treeMap.put("b", 1);
		
		// 假設現在要根據總流量做排序(上行流量upFlow+下行流量downFlow): FlowBean作為key
		// TreeMap有一個帶Comparator比較器參數的constructor，用於可以自訂義比較的規則和順序，通常用於key是類別時
		// 因為類別通常不像基本類型有一個比較的依據
		// 使用匿名宣告的方式來實作介面Comparator的compare()方法，以定義比較的規則
		// 註: 不能顯示地new介面，但可以匿名宣告的方式來實做介面
		// int compare()是介面Comparator中未被實作的方法
		// 若遇到比較的大小相同時，TreeMap會使用先put()進去的值當作最終值，另外一個值會被覆蓋掉
		// 所以這裡再進一步判斷，使用字串電話號碼的大小來判斷大小
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
		
		// 若直接將創建後的FlowBean放到treeMap中，並用foreach來排序，由於用FlowBean當作key，而FlowBean中
		// 沒有定義比較的規則，treeMap會不知道如何排序，會報錯
		for (Entry<FlowBean, String> entry : entrySet) {
			System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
		}
	}
}
