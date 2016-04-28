package qch;

import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MessageDigest {
	public static void main(String[] args) throws Exception {
		String str = "北京市丰台区拉开卡看到过立刻,就挨打啦立刻就,拉开京东方lkja垃,圾股垃圾堆里房价,黑手";
		int index = str.lastIndexOf(",");
		String a = str.substring(0, index);
		String b = str.substring(index + 1);
		
		System.out.println("time=" + refFormatNowDate());

		System.out.println("a=" + a);
		System.out.println("b=" + b);
		System.out.println("md=" + getMD5(str));
	}

	public static String getMD5(String str) {  
		byte buf[] = null;
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            md.update(str.getBytes());
            buf = md.digest();
        } catch (NoSuchAlgorithmException e) {
            return null;
        }

        StringBuffer sb = new StringBuffer(buf.length * 2);
        for (int i = 0; i < buf.length; i++) {
            sb.append(Character.forDigit((buf[i] & 240) >> 4, 16));
            sb.append(Character.forDigit(buf[i] & 15, 16));
        }

        return sb.toString();
	}

	public static String refFormatNowDate() {
		Date nowTime = new Date(System.currentTimeMillis());
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String retStrFormatNowDate = sdFormatter.format(nowTime);

		return retStrFormatNowDate;
	}
}