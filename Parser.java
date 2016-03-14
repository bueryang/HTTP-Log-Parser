import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Locale;

//This program parses HTTP access log and produces output to statistics.txt

public class Parser {

	// Declare required variables
	int numberOfDays, reqPerDay, successful, notModified, found, unsuccessful, localClients, remoteClients;
	long totalBytes, averageBytes, localClientsBytes, remoteClientsBytes;
	long htmlTotal, imagesTotal, soundTotal, videoTotal, formattedTotal, dynamicTotal, otherTotal;
	long htmlBytes, imagesBytes, soundBytes, videoBytes, formattedBytes, dynamicBytes, otherBytes;
	Date startDate;
	Date endDate;
	Dictionary<String, String> fileTypeDict; // Contains file extension - file
												// type information
	
	// Initiate variables
	private void initiate() {
		this.numberOfDays = 0;
		this.reqPerDay = 0;
		this.totalBytes = 0;
		this.averageBytes = 0;
		this.startDate = null;
		this.endDate = null;
		this.successful = 0;
		this.notModified = 0;
		this.unsuccessful = 0;
		this.found = 0;
		this.localClients = 0;
		this.remoteClients = 0;
		this.localClientsBytes = 0;
		this.remoteClientsBytes = 0;

		// Initating variables for Q7 and Q8
		this.htmlTotal = 0;
		this.imagesTotal = 0;
		this.soundTotal = 0;
		this.videoTotal = 0;
		this.formattedTotal = 0;
		this.dynamicTotal = 0;
		this.otherTotal = 0;
		this.htmlBytes = 0;
		this.imagesBytes = 0;
		this.soundBytes = 0;
		this.videoBytes = 0;
		this.formattedBytes = 0;
		this.dynamicBytes = 0;
		this.otherBytes = 0;

		this.fileTypeDict = new Hashtable<String, String>();
		initializeFileType();	
	}

	// Parse the log
	public void Parse(File logFile) {

		String line;
		String[] elements;

		String sourceAddress;
		String timeStr;
		String requestMethod;
		String requestFileName;
		String HTTPversion;
		String responseCode;
		String replySizeInBytes;
		String fileType;
		
		// Initiate class variables
		initiate();

		try {
			// Initiate FileReader for log and FileWriter for debug and output
			BufferedReader br = new BufferedReader(new FileReader(logFile));
			BufferedWriter bw = new BufferedWriter(new FileWriter(
					"statistics.txt"));
			bw.write("BufferedWriter initiated Successfuly\n");
			
			// Read Each line from the log and process output
			int counter = 0;
			while ((line = br.readLine()) != null) {// && counter < 100000) {
				// Skip to the next line if this line has an empty string
				if (line.equals(""))
					continue;

				// Split the line into each element and assign to the variable
				elements = line.split("\\s+");
				
				sourceAddress = elements[0];

				// Skip to the next line if this line contains not equal to 10
				// or 9 elements
								
				if (!(elements.length > 8 && elements.length < 12)){					
					continue;
				}
				
				// If there is more than 1 element in user information, correct the index of
				//other elements
				
				int timeStrIndex = 0;
				for(int i = 0; i< elements.length;i++){
					timeStrIndex = i;
					if (elements[i+1].contains("-0600")) break;
				}

				timeStr = elements[timeStrIndex];				
				this.endDate = parseDate(timeStr);		
				
				requestMethod = elements[timeStrIndex + 2].replaceAll("\"", "");
				requestFileName = elements[timeStrIndex + 3].replaceAll("\"", "");
				
				// If this line is the first or the last line, store date
				// information
				if (counter == 0)
					this.startDate = this.endDate;

				// Parse the date information
				Date currentDate = this.endDate;
				
				// put HTTPversion as empty string if not exist
				if (elements.length > 9) HTTPversion = elements[7];					
				else HTTPversion = elements[6];
					
				if (!(HTTPversion.contains("HTTP") || HTTPversion.contains("1.0"))) HTTPversion = "";
				
				responseCode = elements[elements.length - 2];
				replySizeInBytes = elements[elements.length - 1];
				
				
				/* To generate statistics output*/		
                
				counter++;

				int code = Integer.parseInt(responseCode);
				if (code == 200) {
					this.successful++;
				}
				else if (code == 304) {
					this.notModified++;
				}
				else if (code == 302) {
					this.found++;
				}
				else if (code >=400 && code < 600) {
					this.unsuccessful++;
				}

				if (isInteger(replySizeInBytes) && code < 400) {
					if (sourceAddress.contains("usask.ca") || sourceAddress.startsWith("128.233")) {
						localClients++;
						localClientsBytes += Long.parseLong(replySizeInBytes);
					} else {
						remoteClients++;
						remoteClientsBytes += Long.parseLong(replySizeInBytes);
					}
				}

				System.out.print(sourceAddress + " , " + timeStr + " , " + requestMethod + " , " + requestFileName
						+ " , " + HTTPversion + " , " + responseCode + " , " + replySizeInBytes);
				
				if (!isInteger(replySizeInBytes)){
					System.out.println();
					continue;
				} else {
					totalBytes += Long.parseLong(replySizeInBytes);
				}
				
				fileType = getFileType(requestFileName); 
				System.out.println(" , " + fileType);

				if (fileType == "HTML") {
					htmlBytes += Long.parseLong(replySizeInBytes);
					htmlTotal++;
				}
				else if (fileType == "Images") {
					imagesBytes += Long.parseLong(replySizeInBytes);
					imagesTotal++;
				}
				else if (fileType == "Sound") {
					soundBytes += Long.parseLong(replySizeInBytes);
					soundTotal++;
				}
				else if (fileType == "Video") {
					videoBytes += Long.parseLong(replySizeInBytes);
					videoTotal++;
				}
				else if (fileType == "Formatted") {
					formattedBytes += Long.parseLong(replySizeInBytes);
					formattedTotal++;
				}
				else if (fileType == "Dynamic") {
					dynamicBytes += Long.parseLong(replySizeInBytes);
					dynamicTotal++;
				}
				else if (fileType == "Others") {
					otherBytes += Long.parseLong(replySizeInBytes);
					otherTotal++;
				}

			}

			long dateDiff = this.endDate.getTime() - this.startDate.getTime();
			this.numberOfDays = (int) (dateDiff / (24 * 60 * 60 * 1000));
			this.reqPerDay = counter/numberOfDays;
			this.averageBytes = totalBytes/numberOfDays;

			double totBytes = Round((double)totalBytes/1000000, 2);
			double avBytes = Round((double)averageBytes/1000000, 2);

			bw.newLine();
			bw.write("Start Date is " + this.startDate);
			bw.newLine();
			bw.write("End Date is " + this.endDate);
			bw.newLine();
			bw.write("Total Days is " + numberOfDays);
			bw.newLine();
			bw.write("Average requests per Day is " + this.reqPerDay);
			bw.newLine();
			bw.write("Total MBytes Transfered is " + totBytes);
			bw.newLine();
			bw.write("Average MBytes per day is " + avBytes);
			bw.newLine();
			bw.write("Number of successful (200) requests: " + successful + " (" + (Round((successful/1.0/counter*100), 2)) + "%)");
			bw.newLine();
			bw.write("Number of notModified (304) requests: " + notModified + " (" + (Round((notModified/1.0/counter*100), 2)) + "%)");
			bw.newLine();
			bw.write("Number of found (302) requests: " + found + " (" + (Round((found/1.0/counter*100), 2)) + "%)");
			bw.newLine();
			bw.write("Number of unsuccessful (4XX and 5XX) requests: " + unsuccessful + " (" + (Round((unsuccessful/1.0/counter*100), 2)) + "%)");
			bw.newLine();
			bw.write("Total requests: " + (successful+notModified+found+unsuccessful));
			bw.newLine();
			bw.write("Total requests (count): " + counter);
			bw.newLine();
			bw.write("Local requests: " + localClients + " (" + (Round((localClients/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.newLine();
			bw.write("Total Client Data MBytes: " + localClientsBytes + " (" + (Round((localClientsBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + "%)");
			bw.newLine();
			bw.write("Total Remote Data MBytes: " + remoteClientsBytes + " (" + (Round((remoteClientsBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + "%)");

			// Q7 and Q8
			bw.newLine();
			bw.write("Total HTML Data MBytes: " + htmlBytes + " (" + (Round((htmlBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + 
				"%) - Total HTML requests " + htmlTotal + " (" + (Round((htmlTotal/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.write("  - Average Transfer size = " + Round((htmlBytes/1.0/htmlTotal), 2));
			bw.newLine();
			bw.write("Total Images Data MBytes: " + imagesBytes + " (" + (Round((imagesBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + 
				"%) - Total Images requests " + imagesTotal + " (" + (Round((imagesTotal/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.write("  - Average Transfer size = " + Round((imagesBytes/1.0/imagesTotal), 2));
			bw.newLine();
			bw.write("Total Sound Data MBytes: " + soundBytes + " (" + (Round((soundBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + 
				"%) - Total Sound requests " + soundTotal + " (" + (Round((soundTotal/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.write("  - Average Transfer size = " + Round((soundBytes/1.0/soundTotal), 2));
			bw.newLine();
			bw.write("Total Video Data MBytes: " + videoBytes + " (" + (Round((videoBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + 
				"%) - Total Video requests " + videoTotal + " (" + (Round((videoTotal/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.write("  - Average Transfer size = " + Round((videoBytes/1.0/videoTotal), 2));
			bw.newLine();
			bw.write("Total Formatted Data MBytes: " + formattedBytes + " (" + (Round((formattedBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + 
				"%) - Total Formatted requests " + formattedTotal + " (" + (Round((formattedTotal/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.write("  - Average Transfer size = " + Round((formattedBytes/1.0/formattedTotal), 2));
			bw.newLine();
			bw.write("Total Dynamic Data MBytes: " + dynamicBytes + " (" + (Round((dynamicBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + 
				"%) - Total Dynamic requests " + dynamicTotal + " (" + (Round((dynamicTotal/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.write("  - Average Transfer size = " + Round((dynamicBytes/1.0/dynamicTotal), 2));
			bw.newLine();
			bw.write("Total Others Data MBytes: " + otherBytes + " (" + (Round((otherBytes/1.0/(localClientsBytes+remoteClientsBytes)*100), 2)) + 
				"%) - Total Others requests " + otherTotal + " (" + (Round((otherTotal/1.0/(counter-unsuccessful)*100), 2)) + "%)");
			bw.write("  - Average Transfer size = " + Round((otherBytes/1.0/otherTotal), 2));
			bw.newLine();
			
			br.close();
			bw.close();
			
			/************************************/
			

		} catch (FileNotFoundException e) {
			System.out.println("Invalid Filename");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Cannot Read/write the File");
			e.printStackTrace();
		}
	}

		// Put file types into dictionary
	private void initializeFileType() {

		// Example for each file type
		// You need to add more extensions here
		
		this.fileTypeDict.put("html", "HTML");
		this.fileTypeDict.put("htm", "HTML");
		this.fileTypeDict.put("shtml", "HTML");
		this.fileTypeDict.put("map", "HTML");
		//this.fileTypeDict.put("shtm", "HTML");

		this.fileTypeDict.put("gif", "Images");
		this.fileTypeDict.put("jpeg", "Images");
		this.fileTypeDict.put("jpg", "Images");
		this.fileTypeDict.put("xbm", "Images");
		this.fileTypeDict.put("bmp", "Images");
		this.fileTypeDict.put("rgb", "Images");
		this.fileTypeDict.put("xpm", "Images");


		this.fileTypeDict.put("au", "Sound");
		this.fileTypeDict.put("snd", "Sound");
		this.fileTypeDict.put("wav", "Sound");
		this.fileTypeDict.put("mid", "Sound");
		this.fileTypeDict.put("midi", "Sound");
		this.fileTypeDict.put("lha", "Sound");
		this.fileTypeDict.put("aif", "Sound");
		this.fileTypeDict.put("aiff", "Sound");

		this.fileTypeDict.put("mov", "Video");
		this.fileTypeDict.put("movie", "Video");
		this.fileTypeDict.put("avi", "Video");
		this.fileTypeDict.put("qt", "Video");
		this.fileTypeDict.put("mpeg", "Video");
		this.fileTypeDict.put("mpg", "Video");

		this.fileTypeDict.put("ps", "Formatted");
		this.fileTypeDict.put("eps", "Formatted");
		this.fileTypeDict.put("doc", "Formatted");
		this.fileTypeDict.put("dvi", "Formatted");
		this.fileTypeDict.put("txt", "Formatted");

		this.fileTypeDict.put("cgi", "Dynamic");
		this.fileTypeDict.put("pl", "Dynamic");
		this.fileTypeDict.put("cgi-bin", "Dynamic");
	}

	private String checkResponseCode(String code){
		if (code.equals("200")) return "Successful";
		else if (code.equals("304")) return "Not Modified";
		else if (code.equals("302")) return "Found";
		else  return null;		
	}
	
	// Return file type from file name
	private String getFileType(String URI){
	
		if (URI.endsWith("/") || URI.endsWith(".") || URI.endsWith("..")) return "HTML";
		
		String filename = URI.split("/")[URI.split("/").length-1];
		
		if (filename.contains("?"))	return "Dynamic";			

		String[] filenames = filename.split("\\.");
		String extension = filenames[filenames.length-1];				
		String fileType = fileTypeDict.get(extension.toLowerCase());
		if (fileType == null) return "Others";
		return fileType;		
	}
	

	// Check whether a String is an integer
	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	// Parse date String to Date object
	private Date parseDate(String dateString) {
		SimpleDateFormat DFparser;
		dateString = dateString.replace("[", "");
		// parse the dateString into the Date object
		DFparser = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		try {
			return DFparser.parse(dateString);
		} catch (ParseException e) {
			System.out.println("Incorrect date format");
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	// round floating point
	private static double Round(double Rval, int Rpl) {
		double p = (double) Math.pow(10, Rpl);
		Rval = Rval * p;
		double tmp = Math.round(Rval);
		return (double) tmp / p;
	}
}