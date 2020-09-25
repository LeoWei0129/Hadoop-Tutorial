package hdfs.datacollect;

import java.io.IOException;
import java.util.Properties;

/**
 * 
 * ��ҼҦ��G�i�~�� - �Ҽ{�u�{�w��
 * �bDataCollectionMain���O���A����ӽu�{(timer.schedule()�ҰʡA�]���b�Ĥ@�ӰѼƸ̹�{run())�A�Ĥ@�ӽu�{��getProps()
 * �ɡA�|new Properties()�A�����i��ĤG�ӽu�{�Ұʮɶ����Ĥ@��thread�ܪ�A��getProps()�ɡA�Ĥ@��thread�٦bŪ�����A����
 * if(prop==null)��ĤG��thread�ӻ��]�Otrue�A�ҥH�ĤG��thread�]�|�hnew
 * Properties()�A�o�˴N�|�����Properties����� �ҥH�n�W�[��(lock)�A�]�N�O�P�B�ơA���Ĥ@��thread�bnew
 * Properties()�ɡA���Ӱ϶��A���ĤG��thread���b�~���A����Ĥ@��thread
 * �����Ӱ϶���A�ĤG��thread�~���~�򩹤U����A�o�˥i�H�קK�ĤG��thread�]new
 * Properties()(��if���ĤG��thread�קK�i�Jnew Properties()�϶�)
 * 
 * @author Leo
 * @version 1.0
 * @date 2019�~9��18�� �U��11:46:52
 * @remarks TODO
 *
 */

public class PropertyHolderLazy {
	private static Properties prop = null;

	public static Properties getProps() throws IOException {
		if (prop == null) {
			synchronized (PropertyHolderLazy.class) {
				if (prop == null) {
					prop = new Properties();
					prop.load(PropertyHolderHungry.class.getClassLoader()
							.getResourceAsStream("datacollection.properties"));
				}
			}
		}

		return prop;
	}
}
