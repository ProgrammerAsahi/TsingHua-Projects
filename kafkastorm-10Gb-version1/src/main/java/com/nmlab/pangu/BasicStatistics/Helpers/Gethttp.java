package main.java.com.nmlab.pangu.BasicStatistics.Helpers;

public class Gethttp {
	   public String src_ip;
	   public String dst_ip;
	   public int getcount=0;
	   public int responsecount=0;
	   public long startsec=0;
	   public long startusec=0;
	   public long endusec=0;
	   public long endsec=0;
	   public int length;
	   public String src_hostname="unkown";
	   public String dst_hostname="unkown";
	   public Gethttp(String src_ip,String dst_ip,int getcount,int responsecount,long startsec,long startusec,long endsec,long endusec,int length,String src_hostname,String dst_hostname)
	   {
		   this.src_ip=src_ip;
		   this.dst_ip=dst_ip;
		   this.getcount=getcount;
		   this.responsecount=responsecount;
		   this.startsec=startsec;
		   this.startusec=startusec;//开始时间中的微妙部分
		   this.endsec=endsec;
		   this.endusec=endusec;
		   this.length=length;
		   this.src_hostname=src_hostname;
		   this.dst_hostname=dst_hostname;
	   }
}
