package qch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertTestData {
	private static Configuration _conf = HBaseConfiguration.create();
	private static HTable _hTable = null;

	static {
		_conf.set("hbase.zookeeper.quorum", "dscn31,dscn32,dscn33");
		try {
			_hTable = new HTable(_conf, "qchtest");
		} catch (IOException e) {
	    	System.out.println("[QCH]cann't connect hbase.");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
    	System.out.println("[QCH]main method start.");

    	String array[][] = {
			//基准数据
			{"rk00001", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371222", "ABCD", "444444444444444444", "1", "2016/4/23 11:34:29"},
			//寄件收件调换
			{"rk00002", "lisi",     "134567890111", "01063371222", "ABCD", "444444444444444444", "1", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "2016/4/23 11:34:30"},
			//只有移动电话
			{"rk00003", "zhangsan", "134567890123", "",            "abcd", "333333333333333333", "1", "lisi",     "134567890111", "",            "ABCD", "444444444444444444", "1", "2016/4/23 11:34:31"},
			//只有固定电话
			{"rk00004", "zhangsan", "",             "01063371221", "abcd", "333333333333333333", "1", "lisi",     "",             "01063371222", "ABCD", "444444444444444444", "1", "2016/4/23 11:34:32"},
			//无电话
			{"rk00005", "zhangsan", "",             "",            "abcd", "333333333333333333", "1", "lisi",     "",             "",            "ABCD", "444444444444444444", "1", "2016/4/23 11:34:33"},
			//增加寄件人
			{"rk00006", "wangwu",   "234567890123", "",            "bbbb", "555555555555555555", "1", "lisi",     "134567890111", "",            "ABCD", "444444444444444444", "1", "2016/4/23 11:34:34"},
			//增加收件人
			{"rk00007", "wangwu",   "234567890123", "",            "bbbb", "555555555555555555", "1", "cailiu",   "334567890111", "",            "cccc", "666666666666666666", "1", "2016/4/23 11:34:35"},
			//寄件收件调换
			{"rk00008", "cailiu",   "334567890111", "",            "cccc", "666666666666666666", "1", "wangwu",   "234567890123", "",            "bbbb", "555555555555555555", "1", "2016/4/23 11:34:36"},
			//移动电话不同
			{"rk00009", "zhangsan", "",             "",            "abcd", "333333333333333333", "1", "lisi",     "123456700000", "",            "ABCD", "444444444444444444", "1", "2016/4/23 11:34:42"},
			//固定电话不同
			{"rk00010", "wangwu",   "234567890123", "",            "bbbb", "555555555555555555", "1", "lisi",     "",             "01063371333", "ABCD", "444444444444444444", "1", "2016/4/23 11:34:41"},
			//lisi:移动电话不同，寄件日期相同
			{"rk00011", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890112", "01063371222", "ABCD", "444444444444444444", "1", "2016/4/23 11:34:29"},
			//lisi:移动电话不同，寄件日期不同
			{"rk00012", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890112", "01063371222", "ABCD", "444444444444444444", "1", "2016/4/23 11:35:29"},
			//lisi:固定电话不同，寄件日期相同
			{"rk00013", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371223", "ABCD", "444444444444444444", "1", "2016/4/23 11:34:29"},
			//lisi:固定电话不同，寄件日期不同
			{"rk00014", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371223", "ABCD", "444444444444444444", "1", "2016/4/23 11:44:29"},
			//lisi:详细地址不同，寄件日期相同
			{"rk00015", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371222", "北京1", "444444444444444444", "1", "2016/4/23 11:34:29"},
			//lisi:详细地址不同，寄件日期不同
			{"rk00016", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371222", "北京1", "444444444444444444", "1", "2016/4/23 12:34:29"},
			//lisi:二维码不同，寄件日期相同
			{"rk00017", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371222", "ABCD", "444444444444444444", "2", "2016/4/23 11:34:29"},
			//lisi:二维码不同，寄件日期不同
			{"rk00018", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371222", "ABCD", "444444444444444444", "2", "2016/4/23 21:34:29"},
			//重复数据
			{"rk00019", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "lisi",     "134567890111", "01063371222", "ABCD", "444444444444444444", "1", "2016/4/23 11:34:37"},
			//重复数据
			{"rk00020", "lisi",     "134567890111", "01063371222", "ABCD", "444444444444444444", "1", "zhangsan", "134567890123", "01063371221", "abcd", "333333333333333333", "1", "2016/4/23 11:34:38"},
			//重复数据
			{"rk00021", "zhangsan", "134567890123", "",            "abcd", "333333333333333333", "1", "lisi",     "134567890111", "",            "FFFF", "444444444444444444", "1", "2016/4/23 11:34:39"},
			//重复数据
			{"rk00022", "zhangsan", "",             "01063371221", "abcd", "333333333333333333", "1", "lisi",     "",             "01066666666", "EEEE", "444444444444444444", "1", "2016/4/23 11:34:50"},
			//重复数据
			{"rk00023", "wangwu",   "234567890123", "",            "bbbb", "555555555555555555", "1", "cailiu",   "334567890111", "",            "cccc", "666666666666666666", "1", "2016/4/23 11:34:35"},
			//重复数据
			{"rk00024", "cailiu",   "334567890111", "",            "cccc", "666666666666666666", "1", "wangwu",   "234567890123", "",            "bbbb", "555555555555555555", "1", "2016/4/23 11:34:36"},
		};

		List<Put> puts = new ArrayList<Put>();
		List<Delete> deletes = new ArrayList<Delete>();
		for (int i = 0; i < array.length; i++) {
			String[] columns = array[i];
	    	System.out.println("[QCH]columns[" + i + "]=" + columns.toString());

			Delete delete = new Delete(Bytes.toBytes(columns[0]));
			deletes.add(delete);
	    	
	    	Put put = new Put(Bytes.toBytes(columns[0]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("SEN_NAME"), Bytes.toBytes(columns[1]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("SEN_MOBILE"), Bytes.toBytes(columns[2]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("SEN_PHONE"), Bytes.toBytes(columns[3]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("SEN_ADDRESS"), Bytes.toBytes(columns[4]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("SEN_ID"), Bytes.toBytes(columns[5]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("SEN_QRCODE"), Bytes.toBytes(columns[6]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("REC_NAME"), Bytes.toBytes(columns[7]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("REC_MOBILE"), Bytes.toBytes(columns[8]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("REC_PHONE"), Bytes.toBytes(columns[9]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("REC_ADDRESS"), Bytes.toBytes(columns[10]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("REC_ID"), Bytes.toBytes(columns[11]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("REC_QRCODE"), Bytes.toBytes(columns[12]));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("MAIL_DATETIME"), Bytes.toBytes(columns[13]));
			puts.add(put);
		}
    	System.out.println("[QCH]puts size=" + puts.size());

    	System.out.println("[QCH]Insert Start.");
		try {
			_hTable.delete(deletes);
			_hTable.put(puts);
		} catch (IOException e) {
	    	System.out.println("[QCH]Insert Error. " + e.getMessage());
			return;
		}
    	System.out.println("[QCH]Insert Success");
	}
}