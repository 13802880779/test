import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class hbasetest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub


	}
	
	public static void testput() throws IOException
	{
		HTablePool pool=new HTablePool();
		HTableInterface ht=pool.getTable("test");
		Put p=new Put(Bytes.toBytes("row3"));
		p.add(Bytes.toBytes("cf1"), Bytes.toBytes("col1"), Bytes.toBytes("val3"));
		ht.put(p);
		ht.close();
	}
	
	public static void testget() throws IOException
	{
		HTablePool pool=new HTablePool();
		HTableInterface ht=pool.getTable("test");
		
		Get g=new Get(Bytes.toBytes("row1"));
		g.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
	//	g.ad
		
		Result r=ht.get(g);
		
		
		byte[] val=r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
		System.out.println(Bytes.toString(val));
	//	System.out.println(Bytes.toString(r.get)));
		ht.close();
		
		
		
	}
	
	public static void testdelete() throws IOException
	{
		HTablePool pool =new HTablePool();
		HTableInterface ht=pool.getTable("test");
		Delete d=new Delete(Bytes.toBytes("row3"));
		d.deleteFamily(Bytes.toBytes("cf1"));
		ht.delete(d);
		ht.close();
	}
	
	public static void testscan() throws IOException
	{
		HTablePool pool=new HTablePool();
		HTableInterface ht=pool.getTable("test");
		Scan s=new Scan();
		s.setCaching(100);
		ResultScanner rs=ht.getScanner(s);
		for(Result r:rs)
		{
			System.out.print(Bytes.toString(r.getRow())+"==>"+Bytes.toString(r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("q1"))));
		}
		
		ht.close();
		
	}
	public static void testgetversion() throws IOException
	{
		HTablePool pool=new HTablePool();
		HTableInterface ht=pool.getTable("test");
		Get g=new Get(Bytes.toBytes("row3"));

		g.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
		g.setMaxVersions();//set the max version returned ,Integer.MAXLENGTH
	//	g.setMaxVersions(1);
		
		Result r=ht.get(g);
		List<KeyValue> l=r.getColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1"));
		//System.out.print(Bytes.toString(l.get(0).getValue()));
		//System.out.print(Bytes.toString(l.get(1).getValue()));
		Iterator i=l.iterator();
		KeyValue kv;
		while(i.hasNext())
		{
			kv=(KeyValue) i.next();
			System.out.println(Bytes.toString(kv.getValue()));
		}
		ht.close();
	}

}
