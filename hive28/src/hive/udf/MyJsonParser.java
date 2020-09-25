package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * �o�Ӥ�k�|�bHive���QMapReduce�եΡA�ҥH�o�Ӥ�k�C�@�����O�@�Φbjson�����@��
 */
public class MyJsonParser extends UDF{
	// �������������@�Ӥ�kevaluate()
	public String evaluate(String json, int index){
		// {"movie":"1193","rate":"5","timestamp":"2018-05-11","uid":"1"} 3 7 11 15
		String[] fields = json.split("\"");
	
		return fields[4 * index - 1];
	}
}