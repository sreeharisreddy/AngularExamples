import java.util.Date;
import java.util.Random;

import lombok.Data;

@Data
public class LogSimulator {
	private Random random;
	
	public LogSimulator() {
		random = new Random();
	}

	public final String getRandomLog() {
		long runtime = new Date().getTime();
		String ip = "192.168.2." + random.nextInt(255);
		String msg = runtime + "www.example.com," + ip;
		return msg;
	}
	
	public final String getRandomIP() {
		String ip = "192.168.2." + random.nextInt(255);
		return ip;
	}	
	
	public final String getRandomMessage(final String ip) {
		long runtime = new Date().getTime();
		String msg = runtime + "www.example.com," + ip;
		return msg;
	}	
}
