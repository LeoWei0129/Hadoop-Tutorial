package hdfs.wordcount;

public interface Mapper {
	// Context���t�d�˪F��A��B�z�������G��iContext(�b�̭��HHashMap�Ӹ˪F��)
	public void map(String line, Context context);
}
