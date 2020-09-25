package hdfs.wordcount;

import java.util.HashMap;

public class Context {
	private HashMap<Object, Object> contextMap = new HashMap<>(); // new Context()�ɡAcontextMap�]�Qnew
	
	/**
	 * �⵹���F�詹contextMap�̭��g
	 * @param key
	 * @param value
	 */
	public void write(Object key, Object value) {
		contextMap.put(key, value);
	}
	
	/**
	 * ���okey������value�A�H�P�_�ӭȬO�_�w�s�b��contextMap��
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
