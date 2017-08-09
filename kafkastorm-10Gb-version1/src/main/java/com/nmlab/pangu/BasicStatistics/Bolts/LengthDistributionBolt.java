package main.java.com.nmlab.pangu.BasicStatistics.Bolts;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
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

public class LengthDistributionBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public long time = 0;
	static long slots = 0;
	public long length = 0;
	public long countPacket = 0;
	public long distribution[];
	public long rise=100;
	public long base=100;
	public FileWriter fw ;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("timestamp","throughput","countPacket"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		
		try {
			  fw = new FileWriter("/home/pangu/lengthdistribution");
		} catch (FileNotFoundException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		this.distribution=new long[100];
		for(int i=0;i<100;i++)
		{
			this.distribution[i]=(long)0;
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


				//System.out.println("bsec :"+tuple.getValueByField("sec"));
				//System.out.println("btime:"+time);
				for(int i=0;i<21;i++){
					try {
						fw.write(slots+" "+time+" "+i+" "+this.distribution[i]+"\r\n");
						
					} catch (IOException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
					//System.out.println(slots+" "+time+" "+i+" "+this.distribution[i]);
					}
				//this.outputCollector.emit(new Values(time,length,countPacket));
				
				//关键变量： time代表当前的时间片，throughput代表该时间片的吞吐量，单位byte,countPacket代表分组数，
				//接下来这些变量都会被重置，进入下一个时间片，请在这里进行操作
				length = 0;
				countPacket = 0;
				slots++;
				time = (Long) tuple.getValueByField("sec");
				for(int i=0;i<100;i++)
				{
					this.distribution[i]=(long)0;
				}
			}
			else {
				//System.out.println("nanos :"+tuple.getValueByField("sec"));
					length =(Integer)tuple.getValueByField("len");
					if(length<=base)
					{
						this.distribution[0]+=1;
					}
					for(int i=0;i<19;i++){
						if(length>base+i*rise&&length<=base+(i+1)*rise)
						{
							this.distribution[i+1]+=1;
							break;
						}
					}
					if(length>base+19*rise)
					{
						this.distribution[20]+=1;
					}
				}
        } catch(FailedException e) {
	    	System.out.println("Bolt fail to deal with packet");
	    }  

	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
