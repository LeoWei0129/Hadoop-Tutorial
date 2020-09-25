package zookeeper.distributesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

/**
 * 
 * 服務端的自訂義業務邏輯類別(由服務端所產生的thread)
 * @author Leo
 * @version 1.0
 * @date 2019年10月23日 下午11:03:01
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
			
			System.out.println("業務線程已綁定端口" + port + "準備接受消費端的請求了...");
			
			// 等待接收客戶端那方的請求
			while(true) {
				Socket socket = serverSocket.accept();
				InputStream inputStream = socket.getInputStream(); // 客戶請求的東西
				OutputStream outputStream = (OutputStream) socket.getOutputStream(); // 服務器返回的資訊
				outputStream.write(new Date().toString().getBytes());
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
