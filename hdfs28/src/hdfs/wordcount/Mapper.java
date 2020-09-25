package hdfs.wordcount;

public interface Mapper {
	// Context類負責裝東西，把處理完的結果放進Context(在裡面以HashMap來裝東西)
	public void map(String line, Context context);
}
