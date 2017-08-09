package main.java.com.nmlab.pangu.BasicStatistics.Bolts;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.*; 



public class BasicThoughputBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public long time = 0;
	static long slots = 0;
	public long throughput = 0;
	public long countPacket = 0;
	public FileOutputStream out;
	public FileWriter fw ;
	
	
	public SimpleDateFormat sdf;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("timestamp","throughput","countPacket"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		
		
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// TODO Auto-generated method stub
		try {
			  fw = new FileWriter("/home/pangu/throughput");
		} catch (FileNotFoundException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
		outputCollector.setContext(tuple);  
		try {
			//System.out.println("in execute");
			countPacket++;
			if(time == 0) 
				time =(Long)tuple.getValueByField("sec");
			if((Long) tuple.getValueByField("sec") - time >= 1)
			{
				throughput = throughput +(Integer) tuple.getValueByField("len");
				double  throughput2= ((double)throughput)*8/1000/1000/1000;
				
				
				long lcc_time = Long.valueOf(time);  
	    		String re_StrTime = sdf.format(new Date(lcc_time * 1000L));
				
				System.out.println("bsec :"+tuple.getValueByField("sec"));
				System.out.println("btime:"+re_StrTime);
				System.out.println("bslots:"+slots+"   countPacket:"+countPacket+"  throughput:"+throughput2+"Gb/s");
				
				this.outputCollector.emit(new Values(time,throughput,countPacket));
				
				//关键变量： time代表当前的时间片，throughput代表该时间片的吞吐量，单位byte,countPacket代表分组数，
				//接下来这些变量都会被重置，进入下一个时间片，请在这里进行操作
				throughput = 0;
				countPacket = 0;
				slots++;
				time = (Long) tuple.getValueByField("sec");
			}
			else {
				//System.out.println("nanos :"+tuple.getValueByField("sec"));
				throughput = throughput + (Integer)tuple.getValueByField("len");
				}
        } catch(FailedException e) {
	    	System.out.println("Bolt fail to deal with packet");
	    }  

	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
