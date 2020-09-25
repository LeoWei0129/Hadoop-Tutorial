package hdfs.datacollect;

import java.util.Timer;
import java.util.TimerTask;

public class DataCollectionMain {
	public static void main(String[] args) {
		Timer timer = new Timer();
		
		// 1. TimerTask是一個abstract類，不能被實例化(new)，所以創建一個類(TaskCollection)繼承TimerTask，並實作TimerTask的abstract方法
		// 雖然可以直接new TimerTask(){...}，但因為實作細節較複雜，創建TaskCollection類繼承TimerTask並負責實作
		timer.schedule(new TaskCollection(), 0, 60*60*1000); // 啟動一個任務
		
//		timer.schedule(new TimerTask() { // 匿名的new一個abstract類是可以的
//
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//				
//			}
//			
//		}, 0, 60*60*1000);
		
		// 數據清理調度任務
		timer.schedule(new BackupCleanTask(), 0, 60*60*1000); // 啟動一個任務
	}
}
