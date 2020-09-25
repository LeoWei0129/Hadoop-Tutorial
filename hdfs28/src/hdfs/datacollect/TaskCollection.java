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
	public void run() { // TimerTask��abstract��k
		/**
		 * - �w�ɱ�����x���ؿ� - ����ݭn�Ķ������ - ���ʳo�Ǥ���@�ӫݤW�Ǫ��{�ɥؿ� -
		 * �M���ݤW�ǥؿ��������A�v�@�ǿ��HDFS���ؼи��|�A�P�ɱN�ǿ駹������󲾰ʨ�ƥ��ؿ�
		 */

		try {
			// ����t�m�Ѽ�
			Properties props = PropertyHolderLazy.getProps();

			// �c�y�@��log4j��x��H
			Logger logger = Logger.getLogger("log4j.properties");

			// ��������Ķ��ɪ����
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
			String date = sdf.format(new Date());

			File srcDir = new File(props.getProperty(Constants.LOG_SOURCE_DIR));

			// �ˬd���a������x�ؿ��O�_�s�b�A�p�G���s�b�A�h�Ы�
			if (!srcDir.exists()) {
				srcDir.mkdirs();
			}

			// �C�X��x���ؿ����ݭn�Ķ������
			File[] listFiles = srcDir.listFiles(new FilenameFilter() { // �z�諸�޿�

				@Override
				public boolean accept(File dir, String name) {
					if (name.startsWith(props.getProperty(Constants.LOG_LEGAL_PREFIX))) {
						return true;
					}
					return false;
				}

			});

			// ������x
			logger.info("������p�U���ݭn�Ķ�: " + Arrays.toString(listFiles));

			File toTemplog = new File(props.getProperty(Constants.LOG_TEMPLOG_DIR));

			// �ˬd���a���{�ɥؿ��O�_�s�b�A�p�G���s�b�A�h�Ы�
			if (!toTemplog.exists()) {
				toTemplog.mkdirs();
			}
			// �N�n�Ķ�����󲾰ʨ�ݤW�Ǫ��{�ɥؿ�
			for (File file : listFiles) {
				file.renameTo(new File(props.getProperty(Constants.LOG_TEMPLOG_DIR) + file.getName()));
			}

			// ������x
			logger.info("�W�z��󲾰ʨ�F�ݤW�ǥؿ�: " + toTemplog.getAbsolutePath());

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(new URI(props.getProperty(Constants.HDFS_URI)), conf, "root");
			File[] toTemplogFiles = toTemplog.listFiles();

			// �ˬdHDFS��������ؿ��O�_�s�b�A�p�G���s�b�A�h�Ы�
			Path hdfsDestPath = new Path("/logs/" + date);
			if (!fs.exists(hdfsDestPath)) {
				fs.mkdirs(hdfsDestPath);
			}

			// �ˬd���a���ƥ��ؿ��O�_�s�b�A�p�G���s�b�A�h�Ы�
			File backupDir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR) + date + "/");
			if (!backupDir.exists()) {
				backupDir.mkdirs();
			}

			for (File file : toTemplogFiles) { // toTemplogFiles��listFiles���i�H
				System.out.println(file.getName());
				Path destPath = new Path(hdfsDestPath + "/access_log_" + UUID.randomUUID() + props.getProperty(Constants.HDFS_FILE_SUFFIX));

				// �ǿ����HDFS�ç�W�A"/access_log_" + UUID.randomUUID() + ".log"�O�ɦW
				fs.copyFromLocalFile(new Path(file.getAbsoluteFile().toString()), destPath);

				// ������x
				logger.info("���ǿ��HDFS����: " + file.getAbsolutePath() + "-->" + destPath);

				// �N�ǿ駹������󲾰ʨ�ƥ��ؿ�
				file.renameTo(new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR) + date + "/" + file.getName()));

				// ������x
				logger.info("���ƥ�����: " + file.getAbsolutePath() + "-->" + backupDir);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
