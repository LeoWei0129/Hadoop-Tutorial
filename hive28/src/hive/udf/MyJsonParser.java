package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 這個方法會在Hive中被MapReduce調用，所以這個方法每一次都是作用在json中的一行
 */
public class MyJsonParser extends UDF{
	// 重載父類中的一個方法evaluate()
	public String evaluate(String json, int index){
		// {"movie":"1193","rate":"5","timestamp":"2018-05-11","uid":"1"} 3 7 11 15
		String[] fields = json.split("\"");
	
		return fields[4 * index - 1];
	}
}