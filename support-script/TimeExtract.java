import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Vector;

public class TimeExtract {

	public static void load(String folder) {
		
		BufferedReader br = null;
		BufferedWriter bw = null;
		
		try{
			bw = new BufferedWriter (new FileWriter ("resultsN2.txt"));
			
			bw.write("##G\tdelta\tproportion\talpha\tcases\tknl1TT\tknl2TT\tknl3TT\tdumpTT\tloadDeltaTT\tREAL\tUSER\n");
			
			for(int k = 1; k<10; k++) {
				if(k==3 || k==5 || k == 6 || k==8)
					continue;
				for(int dd =1; dd < 4; dd++) {
					Vector<String> ppSet = new Vector<String>();
					ppSet.add("0.1");
					ppSet.add("1");
					for(String pp : ppSet) {
						Vector<String> alpSet = new Vector<String>();
						alpSet.add("0.5");
						alpSet.add("1");
						alpSet.add("2");
						alpSet.add("5");
						for(String alp : alpSet) {

							Vector<String> caseSet = new Vector<String>();
							caseSet.add("good");
							caseSet.add("bad");
							caseSet.add("mix1");
							caseSet.add("mix2");
							caseSet.add("mix3");
							caseSet.add("mix4");
							
							for(String case1 : caseSet) {
								//tw6-1-1-delta1--po0.1-alp0.5-bad
								String fileName = folder+"/tw6-1-"+k+"-delta"+dd+"--po"+pp+"-alp"+alp+"-"+case1;
								br = new BufferedReader (new FileReader(fileName+".txt"));
								bw.write(""+k+"\t"+dd+"\t"+pp+"\t"+alp+"\t"+case1);
								String line;
								boolean flag = false;
								while(br.ready()) {
									line = br.readLine();
									int indx = line.indexOf("total_time");
									if(indx > 0) {
										String[] tokens = line.substring(indx).split(" ");
										flag = true;
										bw.write("\t" + tokens[1]);
									}
									
									if(flag && line.indexOf("real") >= 0) {
										bw.write("\t" +line.split(" ")[1]);
									}
									if(flag && line.indexOf("user") >= 0) {
										bw.write("\t" +line.split(" ")[1]);
									}
								}
								
								bw.newLine();
//								br.reset();
							}
							
						}
					}
					
				}
			}
			
			
			br.close();
			bw.close();
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			
		}
		
		
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if(args.length < 1) {
			System.out.println("reuqire the folder name");
			return;
		}

		TimeExtract.load(args[0]);

		System.out.println("finish");
		
	}

}
