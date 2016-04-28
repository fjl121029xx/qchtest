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
			{"rk00001", "zhangsan", "134567890123", "010633371221", "abcd", "", "", "lisi", "134567890111", "01063371222", "ABCD", "", ""},
			//瀵寄件收件调换
			{"rk00002", "lisi", "134567890111", "01063371222", "ABCD", "", "", "zhangsan", "134567890123", "010633371221", "abcd", "", ""},
			//只有移动电话
			{"rk00003", "zhangsan", "134567890123", "", "abcd", "", "", "lisi", "134567890111", "", "ABCD", "", ""},
			//只有固定电话
			{"rk00004", "zhangsan", "", "010633371221", "abcd", "", "", "lisi", "", "01063371222", "ABCD", "", ""},
			//无电话
			{"rk00005", "zhangsan", "", "", "abcd", "", "", "lisi", "", "", "ABCD", "", ""},
			//增加寄件人
			{"rk00006", "wangwu", "234567890123", "", "bbbb", "", "", "lisi", "134567890111", "", "ABCD", "", ""},
			//增加收件人
			{"rk00007", "wangwu", "234567890123", "", "bbbb", "", "", "cailiu", "334567890111", "", "cccc", "", ""},
			//寄件收件调换
			{"rk00008", "cailiu", "334567890111", "", "cccc", "", "", "wangwu", "234567890123", "", "bbbb", "", ""},
			//重复数据
			{"rk00009", "zhangsan", "134567890123", "010633371221", "abcd", "", "", "lisi", "134567890111", "01063371222", "ABCD", "", ""},
			//重复数据
			{"rk00010", "lisi", "134567890111", "01063371222", "ABCD", "", "", "zhangsan", "134567890123", "010633371221", "abcd", "", ""},
			//重复数据
			{"rk00011", "zhangsan", "134567890123", "", "abcd", "", "", "lisi", "134567890111", "", "ABCD", "", ""},
			//重复数据
			{"rk00012", "zhangsan", "", "010633371221", "abcd", "", "", "lisi", "", "01063371222", "ABCD", "", ""},
			//重复数据
			{"rk00013", "zhangsan", "", "", "abcd", "", "", "lisi", "", "", "ABCD", "", ""},
			//重复数据
			{"rk00014", "wangwu", "234567890123", "", "bbbb", "", "", "lisi", "134567890111", "", "ABCD", "", ""},
			//重复数据
			{"rk00015", "wangwu", "234567890123", "", "bbbb", "", "", "cailiu", "334567890111", "", "cccc", "", ""},
			//重复数据
			{"rk00016", "cailiu", "334567890111", "", "cccc", "", "", "wangwu", "234567890123", "", "bbbb", "", ""},
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