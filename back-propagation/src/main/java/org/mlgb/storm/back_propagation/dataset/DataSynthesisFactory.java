package org.mlgb.storm.back_propagation.dataset;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class DataSynthesisFactory {
	public static void main(String[] args){
		double[] skewedPercent = {10,20};
		int[] keys = {10000, 20000};
		int recordsNum = 10000000;
//		int recordsNum = 10;
		File zipfDir = new File("C:\\Users\\MrCloud\\Desktop\\zipf-dataset");
		if(!zipfDir.exists()){
			zipfDir.mkdirs();
		}
		for(int i = 0; i < skewedPercent.length; i++){
			double keysPercent[] = new double[5];
			double skew = getExpectedSkew(keys[i], skewedPercent[i]/100, keysPercent);
			System.out.println("\nkeys: " + keys[i]
					+ ", p1%: " + skewedPercent[i]);

			File zipfMetadata = new File(zipfDir.getAbsolutePath()
					+ "/" + String.valueOf(keys[i])
					+ "-" + String.valueOf(skewedPercent[i])
					+ "-metadata"
					+ ".txt");
			if(zipfMetadata.exists()){
				zipfMetadata.delete();
			}
			File zipf = new File(zipfDir.getAbsolutePath()
					+ "/" + String.valueOf(keys[i])
					+ "-" + String.valueOf(skewedPercent[i])
					+ ".txt");
			if(zipf.exists()){
				zipf.delete();
			}
			
			try {
				BufferedWriter bfw = new BufferedWriter(new FileWriter(zipfMetadata));
				bfw.write("messages=" + recordsNum
						+ "\n" + "keys=" + keys[i]
						+ "\n" + "p1(%)=" + keysPercent[0]
						+ "\n" + "p2(%)=" + keysPercent[1]
						+ "\n" + "p3(%)=" + keysPercent[2]
						+ "\n" + "p4(%)=" + keysPercent[3]
						+ "\n" + "p5(%)=" + keysPercent[4]);
				bfw.close();
				
				bfw = new BufferedWriter(new FileWriter(zipf));
				FastZipfGenerator gen = new FastZipfGenerator(keys[i], skew);
				System.out.println("generating...");
				for(int cnt = 0; cnt < recordsNum; cnt++){
					int tmp = gen.next();
//					System.out.println(tmp);
					bfw.write(String.valueOf(cnt) + " " + String.valueOf(tmp));
					bfw.newLine();
				}
				bfw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("task finished.");
	}
	private static double getExpectedSkew(int size, double p1, double[] keysPercent) {
		// TODO Auto-generated method stub
		double skew = 0;
		while(true){
	        double div = 0;
	        for (int i = 1; i <= size; i++)
	        {
	            div += (1 / Math.pow(i, skew));// probably normalization.
	        }
			if(1.0d / div >= p1){
				System.out.println("%p1-p5: " );
				for(int i = 1; i <= 5; i++){
					keysPercent[i-1] = Double.parseDouble(String.format("%.2f", (100*1.0d / Math.pow(i, skew)/ div)));
					System.out.print(String.valueOf(keysPercent[i-1])  + ", ");
				}
				return skew;
			}
			else
				skew += 0.1;
		}
	}
	static class FastZipfGenerator
	{
	    private Random random = new Random(0);
	    private NavigableMap<Double, Integer> map;

	    FastZipfGenerator(int size, double skew)
	    {
	        map = computeMap(size, skew);
	    }

	    private  NavigableMap<Double, Integer> computeMap(
	        int size, double skew)
	    {
	        NavigableMap<Double, Integer> map = 
	            new TreeMap<Double, Integer>();

	        double div = 0;
	        for (int i = 1; i <= size; i++)
	        {
	            div += (1 / Math.pow(i, skew));// probably normalization.
	        }

	        double sum = 0;
	        for(int i=1; i<=size; i++)
	        {
	            double p = (1.0d / Math.pow(i, skew)) / div;// frequency with respect to rank id.
	            sum += p;// frequency ceiling.
	            map.put(sum,  i);
	        }
	        return map;
	    }

	    public int next()
	    {
	        double value = random.nextDouble();// a dice to throw <==> while(!(dice < frequency)){/*throw rank and dice again*/}
	        return map.ceilingEntry(value).getValue();
	    }

	}
}
