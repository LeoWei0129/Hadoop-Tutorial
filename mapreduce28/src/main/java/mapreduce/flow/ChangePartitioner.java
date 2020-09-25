package mapreduce.flow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * 此類會接收Mapper傳輸出去的key-value，並按照此類寫的邏輯決定每個group要分發給哪台機器上的Reducer
 * 所以Partitioner接收key-value必須對應Mapper傳輸出去key-value類型
 * 分發key-value給Reducer不是這個類完成的，是由Map task完成的
 * Map task在完成自己的任務後，會調用這個類(被new)
 * 本類是提供給MapTask用的
 * MapTask通過這個類的getPartition方法，來計算它所產生的每一對kv數據該分發給哪一個reduce task
 * @author Leo
 * @version 1.0
 * @date 2019年10月15日 下午9:11:44
 * @remarks TODO
 *
 */
public class ChangePartitioner extends Partitioner<Text, FlowBean> {
	// 從資料庫調用手機號碼對應的區碼，這裡步實現，直接寫死
	// 手機號碼的前三碼是前綴，第四碼是區碼
	// 每個Map task在調用這個類時，都會擁有共同一份codeMap，這樣只需要訪問資料庫一次
	// String: 號碼前墜
	// Integer: 區碼
	static HashMap<String, Integer> codeMap = new HashMap<String, Integer>();
	
	// 在new這個類時，會加載這個static
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
		return code == null? 5: code; // 5代表若找不到區碼，會放到第6個reducer來執行這個key-value
	}

}
