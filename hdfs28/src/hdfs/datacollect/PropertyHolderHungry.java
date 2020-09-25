package hdfs.datacollect;

import java.io.IOException;
import java.util.Properties;

/**
 * 
 * ��prop�i���ҼҦ��A�קK�O��class�b��getProps()��A���|new Properties() 
 * �j�~��ҼҦ��G�{�ǱҰʮɡA�Nnew Properties()�A���ަ��S����L���եθ����A���|new�@�ӹ��
 * @author Leo
 * @version 1.0
 * @date 2019�~9��18�� �U��11:30:42
 * @remarks TODO
 *
 */

public class PropertyHolderHungry {
	private static Properties prop = new Properties();
	
	static { // �קK�C���]�n�[��ClassLoader�A�]�g���R�A
		/**
		 * �Ĥ@�ӰѼ�InputStream�����n�ε�����|��ܤ��W�A���Y���ᥴ�]��jar�]�Ajar�]�|�Q���N��m�A�o�˵�����|�_����@��
		 * �ϥ�ClassLoader()(���[����)�G�Ұʵ{�ǮɡA�[�����|���D�ثe�o����(PropertyHolder)�b���H�Φb����jar�]�A
		 * ���۴N�i�H�q�o��jar�]���o�t�m���
		 */
		try {
			prop.load(PropertyHolderHungry.class.getClassLoader().getResourceAsStream("datacollection.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Properties getProps() throws Exception { // ���n��ҤƸ����A�����ե�.properties�ɮסA�ҥH�ŧi�@��static��k��Ū���
		return prop; // �R�A��k�̭��u��ե��R�A�ݩ�
	}
}
