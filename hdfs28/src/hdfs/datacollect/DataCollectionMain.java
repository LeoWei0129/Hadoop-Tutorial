package hdfs.datacollect;

import java.util.Timer;
import java.util.TimerTask;

public class DataCollectionMain {
	public static void main(String[] args) {
		Timer timer = new Timer();
		
		// 1. TimerTask�O�@��abstract���A����Q��Ҥ�(new)�A�ҥH�Ыؤ@����(TaskCollection)�~��TimerTask�A�ù�@TimerTask��abstract��k
		// ���M�i�H����new TimerTask(){...}�A���]����@�Ӹ`�������A�Ы�TaskCollection���~��TimerTask�ít�d��@
		timer.schedule(new TaskCollection(), 0, 60*60*1000); // �Ұʤ@�ӥ���
		
//		timer.schedule(new TimerTask() { // �ΦW��new�@��abstract���O�i�H��
//
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//				
//			}
//			
//		}, 0, 60*60*1000);
		
		// �ƾڲM�z�իץ���
		timer.schedule(new BackupCleanTask(), 0, 60*60*1000); // �Ұʤ@�ӥ���
	}
}
