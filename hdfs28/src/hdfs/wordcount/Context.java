package hdfs.wordcount;

import java.util.HashMap;

public class Context {
	private HashMap<Object, Object> contextMap = new HashMap<>(); // new Context()時，contextMap也被new
	
	/**
	 * 把給的東西往contextMap裡面寫
	 * @param key
	 * @param value
	 */
	public void write(Object key, Object value) {
		contextMap.put(key, value);
	}
	
	/**
	 * 取得key對應的value，以判斷該值是否已存在於contextMap中
	 * @param key
	 * @return
	 */
	public Object get(Object key) {
		return contextMap.get(key);
	}
	
	public HashMap<Object, Object> getContextMap(){
		return contextMap;
	}
}
