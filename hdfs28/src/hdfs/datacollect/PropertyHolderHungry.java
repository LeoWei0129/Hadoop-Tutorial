package hdfs.datacollect;

import java.io.IOException;
import java.util.Properties;

/**
 * 
 * 對prop進行單例模式，避免別的class在調getProps()實，都會new Properties() 
 * 餓漢單例模式：程序啟動時，就new Properties()，不管有沒有其他類調用該類，都會new一個實例
 * @author Leo
 * @version 1.0
 * @date 2019年9月18日 下午11:30:42
 * @remarks TODO
 *
 */

public class PropertyHolderHungry {
	private static Properties prop = new Properties();
	
	static { // 避免每次也要加載ClassLoader，也寫成靜態
		/**
		 * 第一個參數InputStream必須要用絕對路徑表示文件名，但若之後打包成jar包，jar包會被任意放置，這樣絕對路徑起不到作用
		 * 使用ClassLoader()(類加載器)：啟動程序時，加載器會知道目前這個類(PropertyHolder)在哪以及在哪個jar包，
		 * 接著就可以從這個jar包取得配置文件
		 */
		try {
			prop.load(PropertyHolderHungry.class.getClassLoader().getResourceAsStream("datacollection.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Properties getProps() throws Exception { // 不要實例化該類，直接調用.properties檔案，所以宣告一個static方法來讀文件
		return prop; // 靜態方法裡面只能調用靜態屬性
	}
}
