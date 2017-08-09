package main.java.com.nmlab.pangu.BasicStatistics.Helpers;

import java.util.ArrayList;

public class SingleLine {
		public ArrayList<Long> timestamp;
		public ArrayList<Long> throughput;
		public ArrayList<Long> countPacket;
		public SingleLine()
		{
			timestamp= new ArrayList<Long>();
			throughput= new ArrayList<Long>();
			countPacket= new ArrayList<Long>();
		}
}
