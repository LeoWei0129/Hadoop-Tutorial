package zookeeper.distributesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

/**
 * 
 * �A�Ⱥݪ��ۭq�q�~���޿����O(�ѪA�ȺݩҲ��ͪ�thread)
 * @author Leo
 * @version 1.0
 * @date 2019�~10��23�� �U��11:03:01
 * @remarks TODO
 *
 */

public class TimeQueryService extends Thread {
	int port = 0;
	
	public TimeQueryService(int port) {
		this.port = port;
	}
	
	@Override
	public void run() {
		try {
			ServerSocket serverSocket = new ServerSocket(port);
			
			System.out.println("�~�Ƚu�{�w�j�w�ݤf" + port + "�ǳƱ������O�ݪ��ШD�F...");
			
			// ���ݱ����Ȥ�ݨ��誺�ШD
			while(true) {
				Socket socket = serverSocket.accept();
				InputStream inputStream = socket.getInputStream(); // �Ȥ�ШD���F��
				OutputStream outputStream = (OutputStream) socket.getOutputStream(); // �A�Ⱦ���^����T
				outputStream.write(new Date().toString().getBytes());
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
