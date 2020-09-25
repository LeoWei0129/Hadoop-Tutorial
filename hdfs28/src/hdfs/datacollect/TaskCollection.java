package hdfs.datacollect;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TaskCollection extends TimerTask {

	@Override
	public void run() { // TimerTask的abstract方法
		/**
		 * - 定時探測日誌源目錄 - 獲取需要採集的文件 - 移動這些文件到一個待上傳的臨時目錄 -
		 * 遍歷待上傳目錄中的文件，逐一傳輸到HDFS的目標路徑，同時將傳輸完成的文件移動到備份目錄
		 */

		try {
			// 獲取配置參數
			Properties props = PropertyHolderLazy.getProps();

			// 構造一個log4j日誌對象
			Logger logger = Logger.getLogger("log4j.properties");

			// 獲取本次採集時的日期
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
			String date = sdf.format(new Date());

			File srcDir = new File(props.getProperty(Constants.LOG_SOURCE_DIR));

			// 檢查本地的源日誌目錄是否存在，如果不存在，則創建
			if (!srcDir.exists()) {
				srcDir.mkdirs();
			}

			// 列出日誌源目錄中需要採集的文件
			File[] listFiles = srcDir.listFiles(new FilenameFilter() { // 篩選的邏輯

				@Override
				public boolean accept(File dir, String name) {
					if (name.startsWith(props.getProperty(Constants.LOG_LEGAL_PREFIX))) {
						return true;
					}
					return false;
				}

			});

			// 紀錄日誌
			logger.info("偵測到如下文件需要採集: " + Arrays.toString(listFiles));

			File toTemplog = new File(props.getProperty(Constants.LOG_TEMPLOG_DIR));

			// 檢查本地的臨時目錄是否存在，如果不存在，則創建
			if (!toTemplog.exists()) {
				toTemplog.mkdirs();
			}
			// 將要採集的文件移動到待上傳的臨時目錄
			for (File file : listFiles) {
				file.renameTo(new File(props.getProperty(Constants.LOG_TEMPLOG_DIR) + file.getName()));
			}

			// 紀錄日誌
			logger.info("上述文件移動到了待上傳目錄: " + toTemplog.getAbsolutePath());

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(new URI(props.getProperty(Constants.HDFS_URI)), conf, "root");
			File[] toTemplogFiles = toTemplog.listFiles();

			// 檢查HDFS中的日期目錄是否存在，如果不存在，則創建
			Path hdfsDestPath = new Path("/logs/" + date);
			if (!fs.exists(hdfsDestPath)) {
				fs.mkdirs(hdfsDestPath);
			}

			// 檢查本地的備份目錄是否存在，如果不存在，則創建
			File backupDir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR) + date + "/");
			if (!backupDir.exists()) {
				backupDir.mkdirs();
			}

			for (File file : toTemplogFiles) { // toTemplogFiles或listFiles都可以
				System.out.println(file.getName());
				Path destPath = new Path(hdfsDestPath + "/access_log_" + UUID.randomUUID() + props.getProperty(Constants.HDFS_FILE_SUFFIX));

				// 傳輸文件到HDFS並改名，"/access_log_" + UUID.randomUUID() + ".log"是檔名
				fs.copyFromLocalFile(new Path(file.getAbsoluteFile().toString()), destPath);

				// 紀錄日誌
				logger.info("文件傳輸到HDFS完成: " + file.getAbsolutePath() + "-->" + destPath);

				// 將傳輸完成的文件移動到備份目錄
				file.renameTo(new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR) + date + "/" + file.getName()));

				// 紀錄日誌
				logger.info("文件備份完成: " + file.getAbsolutePath() + "-->" + backupDir);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
