package main.java.com.nmlab.pangu.BasicStatistics.Bolts;

import java.util.Map;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
//import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;

public class RedisBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public String name=null;
	int count=0;
	int count_second=0;
	int count_minute=0;
	boolean time_minute=false;
	long sum_throuput=0;
	long sum_count=0;
	JedisPool pool;
	Jedis jedis;
	String result_throughput,result_countPac,result_throughput_minute,result_countPac_minute,result_second,result_minute;

	SimpleDateFormat sdf;
	public RedisBolt(String name)
	{
		this.name=name;
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		this.pool = new JedisPool(new JedisPoolConfig(), "2001:da8:a0:500::1:7");
		jedis = pool.getResource();
		result_throughput="";
		result_countPac="";
		result_throughput_minute="";
		result_countPac_minute="";
		result_minute="";
		result_second="";
		count=0;
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
		outputCollector.setContext(input); 
		try {
			String throughput = Long.toString((Long)input.getValueByField("throughput")) ;
			String countPacket = Long.toString((Long) input.getValueByField("countPacket"));
			long time=(Long)input.getValueByField("timestamp");
			//time=time+8*60*60;//转换成北京时间
			if(time%60==0)
			{
				time_minute=true;
			}
	    	if (time_minute==true) {
				//System.out.println("timestamp:"+input.getValueByField("timestamp")+" thoughput: "+input.getValueByField("throughput")+ " countPacket:" +input.getValueByField("countPacket")) ;
				//fw.write("timestamp:"+tuple.getValue(0)+" flow: "+tuple.getValue(1));
	    		long lcc_time = Long.valueOf(time);  
	    		String re_StrTime = sdf.format(new Date(lcc_time * 1000L));
				count++;
				if (count_second < 60) {
					sum_throuput = sum_throuput
							+ (Long) input.getValueByField("throughput");
					sum_count = sum_count
							+ (Long) input.getValueByField("countPacket");
					count_second++;
				} else {
					count_minute++;
					if (count_minute <= 60*24) {
						result_throughput_minute += "/"
								+ Long.toString(sum_throuput);
						result_countPac_minute += "/"
								+ Long.toString(sum_count);
						result_minute += "/"
								+re_StrTime;
					} else {
						int secPos = result_throughput_minute.indexOf("/", 1);
						result_throughput_minute = result_throughput_minute
								.substring(secPos);
						result_throughput_minute += "/"
								+ Long.toString(sum_throuput);
						secPos = result_countPac_minute.indexOf("/", 1);
						result_countPac_minute = result_countPac_minute
								.substring(secPos);
						result_countPac_minute += "/"
								+ Long.toString(sum_count);
						secPos = result_minute.indexOf("/", 1);
						result_minute = result_minute
								.substring(secPos);
						result_minute += "/"
								+re_StrTime;
					}
					jedis.set(this.name+"_throuput_minute", result_throughput_minute);
					jedis.set(this.name+"_countPacket_minute", result_countPac_minute);
					jedis.set(this.name+"_minute", result_minute);
					sum_throuput = (Long) input.getValueByField("throughput");
					sum_count = (Long) input.getValueByField("countPacket");
					count_second = 1;
				}
				if (count <= 60*60) {
					result_throughput += "/" + throughput;
					result_countPac += "/" + countPacket;
					result_second+="/"+re_StrTime;
				} else {
					int secPos = result_throughput.indexOf("/", 1);
					result_throughput = result_throughput.substring(secPos);
					result_throughput += "/" + throughput;
					secPos = result_countPac.indexOf("/", 1);
					result_countPac = result_countPac.substring(secPos);
					result_countPac += "/" + countPacket;
					secPos = result_second.indexOf("/", 1);
					result_second = result_second.substring(secPos);
					result_second += "/" + re_StrTime;
				}
				jedis.set(this.name+"_throuput_second", result_throughput);
				jedis.set(this.name+"_countPacket_second", result_countPac);
				jedis.set(this.name+"_second",result_second);
				//System.out.println(jedis.get("timestamp"));
			}
	    	
	    	//System.out.println(result);
	    	
	    } catch(FailedException e) {  
	    	System.out.println("fail to deal with packet");
	    }  
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		 if (jedis != null) {
	    	    jedis.quit();
	    	  }
	    
	    pool.destroy();
	}

}
