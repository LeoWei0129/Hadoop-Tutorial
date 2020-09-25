package hdfs.datacollect;

import java.io.IOException;
import java.util.Properties;

/**
 * 
 * 單例模式：懶漢式 - 考慮線程安全
 * 在DataCollectionMain類別中，有兩個線程(timer.schedule()啟動，因為在第一個參數裡實現run())，第一個線程調getProps()
 * 時，會new Properties()，但有可能第二個線程啟動時間離第一個thread很近，調getProps()時，第一個thread還在讀取文件，此時
 * if(prop==null)對第二個thread來說也是true，所以第二個thread也會去new
 * Properties()，這樣就會有兩個Properties的實例 所以要增加鎖(lock)，也就是同步化，讓第一個thread在new
 * Properties()時，鎖住該區塊，讓第二個thread等在外面，等到第一個thread
 * 完成該區塊後，第二個thread才能繼續往下執行，這樣可以避免第二個thread也new
 * Properties()(用if讓第二個thread避免進入new Properties()區塊)
 * 
 * @author Leo
 * @version 1.0
 * @date 2019年9月18日 下午11:46:52
 * @remarks TODO
 *
 */

public class PropertyHolderLazy {
	private static Properties prop = null;

	public static Properties getProps() throws IOException {
		if (prop == null) {
			synchronized (PropertyHolderLazy.class) {
				if (prop == null) {
					prop = new Properties();
					prop.load(PropertyHolderHungry.class.getClassLoader()
							.getResourceAsStream("datacollection.properties"));
				}
			}
		}

		return prop;
	}
}
