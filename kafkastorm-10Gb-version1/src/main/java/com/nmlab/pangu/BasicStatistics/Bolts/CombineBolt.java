package main.java.com.nmlab.pangu.BasicStatistics.Bolts;

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

import javax.swing.text.html.HTMLDocument.Iterator;

import main.java.com.nmlab.pangu.BasicStatistics.Helpers.SingleLine;



public class CombineBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public long time = 0;
	static long slots = 0;
	public long throughput = 0;
	public long countPacket = 0;
	
	public long total_time=0;
	public long total_throughput=0;
	public long total_countPacket=0;
	
	public FileOutputStream out;
	public FileWriter fw ;
	
	SingleLine singleline[]=new SingleLine[2];
	
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("timestamp","throughput","countPacket"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		
		
		for(int i=0;i<singleline.length;i++)
		{
			singleline[i]=new SingleLine();
		}
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
		String name=tuple.getSourceComponent();
		try {
			fw.write("tuple.getSourceComponent():"+name+"\r\n");
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
		if(name.equals("BasicThroughputBolt1")){
			time =(Long)tuple.getValueByField("timestamp");
			throughput = (Long)tuple.getValueByField("throughput");
			countPacket= (Long) tuple.getValueByField("countPacket");
			singleline[0].timestamp.add(time);			
			singleline[0].throughput.add(throughput);
			singleline[0].countPacket.add(countPacket);
			try {
				fw.write("BasicThroughputBolt1time:"+(int) time+"\r\n");
			} catch (IOException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
			
			
		}
		if(name.equals("BasicThroughputBolt2")){
			time =(Long)tuple.getValueByField("timestamp");
			throughput = (Long)tuple.getValueByField("throughput");
			countPacket= (Long) tuple.getValueByField("countPacket");
			singleline[1].timestamp.add(time);
			singleline[1].throughput.add(throughput);
			singleline[1].countPacket.add(countPacket);
			try {
				fw.write("BasicThroughputBolt2time:"+(int) time+"\r\n");
			} catch (IOException e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
		}
		
		if(total_time==0)
		{
			if(singleline[0].timestamp.size()!=0&&singleline[1].timestamp.size()!=0)
			{
				
					long tmp=Max(singleline[0].timestamp.get(0),singleline[1].timestamp.get(0));
					
					try {
						fw.write("singleline[0].timestamp.get(0):"+(long) singleline[0].timestamp.get(0)+"\r\n");
						fw.write("singleline[1].timestamp.get(0):"+(long) singleline[1].timestamp.get(0)+"\r\n");
						fw.write("Max(singleline[0].timestamp.get(0),singleline[1].timestamp.get(0)):"+tmp+"\r\n");
						
					} catch (IOException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
					if(singleline[0].timestamp.get(0)!=tmp)
					{
						singleline[0].timestamp.remove(0);
						singleline[0].throughput.remove(0);
						singleline[0].countPacket.remove(0);
					}
					if(singleline[1].timestamp.get(0)!=tmp)
					{
						singleline[1].timestamp.remove(0);
						singleline[1].throughput.remove(0);
						singleline[1].countPacket.remove(0);
					}
			}
			if (singleline[0].timestamp.size()!=0&&singleline[1].timestamp.size()!=0) {
				if (singleline[0].timestamp.get(0).equals(singleline[1].timestamp.get(0))) {
					total_time = singleline[0].timestamp.get(0);
					try {
						fw.write("total_time:"+total_time+"\r\n");
												
					} catch (IOException e) {
						// TODO 自动生成的 catch 块
						e.printStackTrace();
					}
					total_throughput = singleline[0].throughput.get(0)+ singleline[1].throughput.get(0);
					total_countPacket = singleline[0].countPacket.get(0)+ singleline[1].countPacket.get(0);
					this.outputCollector.emit(new Values(total_time,total_throughput, total_countPacket));

					singleline[0].timestamp.remove(0);
					singleline[0].throughput.remove(0);
					singleline[0].countPacket.remove(0);

					singleline[1].timestamp.remove(0);
					singleline[1].throughput.remove(0);
					singleline[1].countPacket.remove(0);

					total_time = total_time + 1;
				}
			}
		}else
		{
			if(singleline[0].timestamp.size()!=0&&singleline[1].timestamp.size()!=0)
			{
				if(singleline[0].timestamp.get(0).equals(total_time)&&singleline[1].timestamp.get(0).equals(total_time))
				{
					total_throughput=singleline[0].throughput.get(0)+singleline[1].throughput.get(0);
					total_countPacket=singleline[0].countPacket.get(0)+singleline[1].countPacket.get(0);
					this.outputCollector.emit(new Values(total_time,total_throughput,total_countPacket));
					
					singleline[0].timestamp.remove(0);
					singleline[0].throughput.remove(0);
					singleline[0].countPacket.remove(0);
					
					singleline[1].timestamp.remove(0);
					singleline[1].throughput.remove(0);
					singleline[1].countPacket.remove(0);
					
					total_time=total_time+1;
				}else
				{
					if(singleline[0].timestamp.get(0).equals(total_time)==false)
					{
						try {
							fw.write("1 have error\r\n");
							total_time=0;
						} catch (IOException e) {
							// TODO 自动生成的 catch 块
							e.printStackTrace();
						}
					}
					if(singleline[1].timestamp.get(0).equals(total_time)==false)
					{
						try {
							fw.write("2 have error\r\n");
							total_time=0;
						} catch (IOException e) {
							// TODO 自动生成的 catch 块
							e.printStackTrace();
						}
					}
					
				}
			}
		}
				 

	}
	public long Max(long a,long b)
	{
		if(a>=b)
		{
			return a;
		}else{
			return b;
		}
	}
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
