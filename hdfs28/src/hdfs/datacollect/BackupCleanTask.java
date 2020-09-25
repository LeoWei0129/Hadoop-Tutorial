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
			// 1. 探測備份目錄
			File backupBaseDir = new File("D:/Apache Ecosystem/logs/backup");
			File[] dateBackupDir = backupBaseDir.listFiles();
			
			// 2. 判斷備份日期子目錄是否已超過24小時
			for (File file : dateBackupDir) {
				long time = sdf.parse(file.getName()).getTime();
				if(now - time > 24 * 60 * 60 * 1000L) {
					FileUtils.deleteDirectory(file); // 遞歸刪除目錄下的文件，比file.delete()適合
					
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		

	}

}
