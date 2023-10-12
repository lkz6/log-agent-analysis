/*
 * Author: 47035
 * Date: 2023/10/11 16:26
 * FileName: DateTimeTest
 * Description:
 */


import com.hzx.common.DatePattern;
import com.hzx.util.DateUtils;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class DateTimeTest {
	private final static SimpleDateFormat fmt2;
	static {
		fmt2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		fmt2.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
	}
	public static void main(String[] args) {
		
		String message = "das|das||";
		
		if (message.contains("\\|")){
			
			System.out.println("-----");
		}
		
	}
}
