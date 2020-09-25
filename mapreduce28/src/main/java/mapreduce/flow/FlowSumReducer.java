package mapreduce.flow;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Reducer泛型的前兩個參數型別，要對應mapper中context.write()的參數型別順序
 * 將Reducer寫進檔案的值以FlowBean封裝起來，可以再透過toString()取得這些值(第四個泛型參數)
 * @author Leo
 * @version 1.0
 * @date 2019年10月7日 下午9:53:48
 * @remarks TODO
 *
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	/**
	 * key: 某個電話號碼，這邊的key已經是由Mapper傳過來的phone了
	 * values: 這個電話號碼所產生的所有訪問記錄中的流量數據
	 * 
	 * (EX):
	 * <0954123486, flowBean1>, <0954123486, flowBean2>, <0954123486, flowBean3>...
	 */
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// Iterable支持foreach，所以不用多一部透過取得迭代器(透過hasNext()來迭代)，如下
//		Iterator<FlowBean> iter = values.iterator();
//		while(iter.hasNext()) {
//			// do something...
//		}
		
		int upSum = 0;
		int downSum = 0;
		
		for (FlowBean flowBean : values) {
			upSum += flowBean.getUpFlow();
			downSum += flowBean.getDownFlow();
		}
		
		context.write(new Text(key.toString()), new FlowBean(upSum, downSum, key.toString()));
	}
}
