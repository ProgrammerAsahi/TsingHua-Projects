package main.java.com.nmlab.pangu.BasicStatistics.Helpers;

import java.util.TreeMap;

import com.sun.javafx.collections.MappingChange.Map;

public class Application {
	private int httplength=0;
	private int httpcount=0;
	private int ftplength=0;
	private int ftpcount=0;
	private int httpslength=0;
	private int httpscount=0;
	private int dnslength=0;
	private int dnscount=0;
	private int ntplength=0;
	private int ntpcount=0;
	private int sockslength=0;
	private int sockscount=0;
	
	public TreeMap<String,Integer> lengthsort;
	public TreeMap<String,Integer> countsort;
    
	public Application()
	{
		lengthsort=new TreeMap<String, Integer>();

		countsort= new TreeMap<String,Integer>();
	}
	
	public void Setlength(String name,int length)
	{
		if(name=="http"){httplength=httplength+length;}
		if(name=="ftp"){ftplength=ftplength+length;}
		if(name=="https"){httpslength=httpslength+length;}
		if(name=="dns"){dnslength=dnslength+length;}
		if(name=="ntp"){ntplength=ntplength+length;}
		if(name=="socks"){sockslength=sockslength+length;}
	}
	public int Getlength(String name)
	{
		if(name=="http"){return httplength;}
		if(name=="ftp"){return ftplength;}
		if(name=="https"){return httpslength;}
		if(name=="dns"){return dnslength;}
		if(name=="ntp"){return ntplength;}
		if(name=="socks"){return sockslength;}
		return 0;
	}
	public void Setcount(String name,int count)
	{
		if(name=="http"){httpcount=httpcount+count;}
		if(name=="ftp"){ftpcount=ftpcount+count;}
		if(name=="https"){httpscount=httpscount+count;}
		if(name=="dns"){dnscount=dnscount+count;}
		if(name=="ntp"){ntpcount=ntpcount+count;}
		if(name=="socks"){sockscount=sockscount+count;}
	}
	
	public int Getcount(String name)
	{
		if(name=="http"){return httpcount;}
		if(name=="ftp"){return ftpcount;}
		if(name=="https"){return httpscount;}
		if(name=="dns"){return dnscount;}
		if(name=="ntp"){return ntpcount;}
		if(name=="socks"){return sockscount;}
		return 0;
	}
	public String TCP_portmap(int src_port,int dst_port)
	{
		switch(src_port)
		{
			case 443:return "https";
			case 80:return "http";
			case 21:return "ftp";
			case 53:return "dns";
			case 1080:return "socks";
		}
		switch(dst_port)
		{
		    case 443:return "https";
			case 80:return "http";
			case 21:return "ftp";
			case 53:return "dns";
			case 1080:return "socks";
		}
		return null;
	}
	public String UDP_portmap(int src_port,int dst_port)
	{
		switch(src_port)
		{
			case 443:return "https";
			case 53:return "dns";
			case 123:return "ntp";
			case 1080:return "socks";
		}
		switch(dst_port)
		{
		    case 443:return "https";
		    case 53:return "dns";
		    case 123:return "ntp";
		    case 1080:
		}
		return null;
	}
	public void lengthsort()
	{
		lengthsort.put("http",Getlength("http"));
		lengthsort.put("ftp",Getlength("ftp"));
		lengthsort.put("https",Getlength("https"));
		lengthsort.put("dns",Getlength("dns"));
		lengthsort.put("ntp",Getlength("ntp"));
		lengthsort.put("socks",Getlength("socks"));
	}
	public void countsort()
	{
		countsort.put("http",Getcount("http"));
		countsort.put("ftp",Getcount("ftp"));
		countsort.put("https",Getcount("https"));
		countsort.put("dns",Getcount("dns"));
		countsort.put("ntp",Getcount("ntp"));
		countsort.put("socks",Getcount("socks"));
	}
	public void Initapplication()
	{
		httplength=0;
		httpcount=0;
		ftplength=0;
		ftpcount=0;
		httpslength=0;
		httpscount=0;
		dnslength=0;
		dnscount=0;
		ntplength=0;
		ntpcount=0;
		sockslength=0;
		sockscount=0;
		lengthsort.clear();
		countsort.clear();
	}
}