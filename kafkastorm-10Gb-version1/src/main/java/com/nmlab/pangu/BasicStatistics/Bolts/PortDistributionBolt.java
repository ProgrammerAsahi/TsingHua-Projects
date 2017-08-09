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



public class PortDistributionBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public long time = 0;
	static long slots = 0;	
	String buffer="";
	public FileOutputStream out;
	public FileWriter fw;
	public SimpleDateFormat sdf;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("timestamp","js_code"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		
		
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		// TODO Auto-generated method stub
		try {			
			fw = new FileWriter("F://My Software//eclipse//workspace-jee//kafkastorm-10Gb-version1//JavaScript_Code.txt",false);			
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
			if(time == 0) 
				time =(Long)tuple.getValueByField("sec");
			if((Long) tuple.getValueByField("sec") - time >= 1)
			{
				buffer=tuple.getValueByField("js_code").toString();
				try {
						fw.write(buffer);
						fw.flush();					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				this.outputCollector.emit(new Values(time,buffer));
				
				//关键变量： time代表当前的时间片，throughput代表该时间片的吞吐量，单位byte,countPacket代表分组数，
				//接下来这些变量都会被重置，进入下一个时间片，请在这里进行操作
				buffer="";
				slots++;
				time = (Long) tuple.getValueByField("sec");
			}
        } catch(FailedException e) {
	    	System.out.println("Bolt fail to deal with packet");
	    } 
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
