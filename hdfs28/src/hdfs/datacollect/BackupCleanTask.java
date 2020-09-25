package hdfs.datacollect;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;

public class BackupCleanTask extends TimerTask {

	@Override
	public void run() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		long now = new Date().getTime();
		
		try {
			// 1. �����ƥ��ؿ�
			File backupBaseDir = new File("D:/Apache Ecosystem/logs/backup");
			File[] dateBackupDir = backupBaseDir.listFiles();
			
			// 2. �P�_�ƥ�����l�ؿ��O�_�w�W�L24�p��
			for (File file : dateBackupDir) {
				long time = sdf.parse(file.getName()).getTime();
				if(now - time > 24 * 60 * 60 * 1000L) {
					FileUtils.deleteDirectory(file); // ���k�R���ؿ��U�����A��file.delete()�A�X
					
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		

	}

}
