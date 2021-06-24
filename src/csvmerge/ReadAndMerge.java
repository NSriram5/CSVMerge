package csvmerge;

import java.util.Arrays;
import java.util.ArrayList;
//import java.util.Formatter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;

/**
 * A helper class that will manage a csv data merge
 * @author NSriram
 *
 */
public class ReadAndMerge {
	private static final int BATCHLENGTH = 100;
	private static final String PRIMARYKEY = "project_id";
	private HashSet<String> duplicateLookup = new HashSet<>();
	private String[] inputFileNames;
	private HashMap<String,Integer> columns = null;
	private int primaryKeyColumn;
	private int venueColumn;
	private int yearColumn;
	private int[] currFileColumnPlacement = new int[0];
	private BufferedReader bufferedReader = null;
	private BufferedWriter bufferedWriter = null;
	private BufferedWriter duplicateBufferedWriter = null;
	private String keyword = "";

	public ReadAndMerge(String[] inputFileNames) throws PrimaryKeyNotFoundException {
		this.inputFileNames = inputFileNames;
		columns = this.buildColumnMap(inputFileNames);
		
		this.findPrimaryKeyColumns();
	}
	
	public ReadAndMerge(String[] inputFileNames, String keyword) throws PrimaryKeyNotFoundException {
		this.inputFileNames = inputFileNames;
		this.keyword = keyword;
		columns = this.buildColumnMap(inputFileNames);
		
		this.findPrimaryKeyColumns();
	}

	/**
	 * A helper process that populates an pointer list of which columns are primary key columns
	 * @throws PrimaryKeyNotFoundException
	 */
	private void findPrimaryKeyColumns() throws PrimaryKeyNotFoundException {
		if (!columns.containsKey(PRIMARYKEY)) {
			throw new PrimaryKeyNotFoundException(PRIMARYKEY);
		}
		if (!columns.containsKey("close_date")) {
			throw new PrimaryKeyNotFoundException("close_date");
		}
		if (!columns.containsKey("product_stage")) {
			throw new PrimaryKeyNotFoundException("product_stage");
		}
		primaryKeyColumn = columns.get(PRIMARYKEY);
		venueColumn = columns.get("product_stage");
		yearColumn = columns.get("close_date");
	}

	/**
	 * A process that takes a batch of CSV files from the instance memory and merges them into one
	 * output file
	 * @param outputFileName the output file that will be merged
	 */
	public void mergeCSVFiles(String outputFileName) {
		String line = "";
		try {
			FileWriter f1 = new FileWriter(outputFileName,false);
			FileWriter f2 = new FileWriter("duplicatesFound.csv",false);
			f1.close();
			f2.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try{
			this.bufferedWriter = new BufferedWriter(new FileWriter(outputFileName,true));
			this.duplicateBufferedWriter = new BufferedWriter(
				new FileWriter("duplicatesFound.csv",true));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch	(IOException e) {
			e.printStackTrace();
		} 
		
		StringBuilder headerRow = new StringBuilder();
		for (String key: columns.keySet()) {
			headerRow.append(key + ",");
		}
		headerRow.deleteCharAt(headerRow.length()-1);
		try{
			bufferedWriter.write(headerRow.toString());
			duplicateBufferedWriter.write(headerRow.toString());
			bufferedWriter.newLine();
			duplicateBufferedWriter.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < inputFileNames.length; i++) {
			System.out.println("Reading file: " + inputFileNames[i]);
			try {this.readFileIntoOutputFile(inputFileNames[i], outputFileName);
			} catch (ColumnSeparationMismatch e) {
				e.printStackTrace();
			}
		}
		try {
			bufferedWriter.close();
			duplicateBufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Helper function that writes files to the output file with an input file
	 * @param inputFileName the filename of the csv file that will be merged
	 * @param outputFileName the filename of the common merge csv file
	 */
	private void readFileIntoOutputFile(String inputFileName,String outputFileName) throws ColumnSeparationMismatch {
		boolean done = false;
		try{
			this.bufferedReader = new BufferedReader(new FileReader(inputFileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch	(IOException e) {
			e.printStackTrace();
		} 

		int currWriteLine = 0;
		String line = "";
		try {
			line = this.bufferedReader.readLine();			
		} catch (IOException e) {
			e.printStackTrace();
		}
		handleFirstLine(line);
		try {
			line = this.bufferedReader.readLine();			
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (line != null) {
			String[] lines = new String[BATCHLENGTH];
			
			while (currWriteLine < BATCHLENGTH && line != null) {
				String[] incomingDataColumns = line.split("\",\"");
				if (line.matches("(.*)\\[(.*)(,)(.*)\\](.*)") && !line.matches("(.*):\\[")) {
					//System.out.println("Brackets detected");
					incomingDataColumns = this.bracketCombine(incomingDataColumns);
				}
				while (incomingDataColumns.length < this.currFileColumnPlacement.length) {
					String appendPart = "";
					try {
						appendPart = bufferedReader.readLine();						
					} catch (IOException e) {
						e.printStackTrace();
					}
					line = line + appendPart;
					incomingDataColumns = line.split("\",\"");
					if (line.matches("(.*)\\[(.*)(,)(.*)\\](.*)") && !line.matches("(.*):\\[")) {
						incomingDataColumns = this.bracketCombine(incomingDataColumns);
					}
				}

				
				String[] expandedDataColumns = new String[columns.size()];
				Arrays.fill(expandedDataColumns,"\"null\"");
				String duplicateComparison = new String();
				for (int i = 0; i < incomingDataColumns.length; i++) {
					if (i == 0) {
						incomingDataColumns[i] = incomingDataColumns[i] + "\"";
					} else if ( i == incomingDataColumns.length - 1) {
						incomingDataColumns[i] = "\"" + incomingDataColumns[i];
					} else {
						incomingDataColumns[i] = "\"" + incomingDataColumns[i] + "\"";
					}
				}

				if (incomingDataColumns.length != this.currFileColumnPlacement.length) {
					throw new ColumnSeparationMismatch(line);
				}

				for (int i = 0; i < incomingDataColumns.length; i++) {
					expandedDataColumns[this.currFileColumnPlacement[i]] = incomingDataColumns[i];
				}

				StringBuilder dataLine = new StringBuilder();
				dataLine.append("\"");
				for (int i = 0; i < expandedDataColumns.length; i++) {
					if (i == expandedDataColumns.length -1) {
						dataLine.append(expandedDataColumns[i]);
					} else {
						dataLine.append(expandedDataColumns[i] + "\",\"");
					}
				}
				dataLine.append("\"");
				line = dataLine.toString();

				
				duplicateComparison = expandedDataColumns[primaryKeyColumn];

				if (duplicateLookup.contains(duplicateComparison)) {
					try {
						duplicateBufferedWriter.write(line);
						duplicateBufferedWriter.newLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else {
					lines[currWriteLine] = line;
					duplicateLookup.add(duplicateComparison);
					if (this.keyword != "" && line.matches("(.*)"+this.keyword+"(.*)")) {
						System.out.println("This is a keyword match: ");
						System.out.println("Venue: " + expandedDataColumns[venueColumn]);
						System.out.println("Date: " + expandedDataColumns[yearColumn]);
					}
					currWriteLine++;
				}
				try {
					line = bufferedReader.readLine();					
				} catch (IOException e) {
					e.printStackTrace();
				}


			}
			System.out.println("Batch read is complete writing lines to merge file");
			this.writeLinesToFile(this.bufferedWriter, lines);
			currWriteLine = 0;
		}
		try {
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

	}	
	
	/**
	 * Returns a map of the columns used across all the files (union)
	 * @param inputFileNames the names of the data files to have columns merged
	 * @return a hashmap of the column names (String) and placement in the column ordering
	 */
	private HashMap<String,Integer> buildColumnMap(String[] inputFileNames) {
		BufferedReader bufferedReader = null;
		HashMap<String,Integer> columns = new HashMap<>();
		String line = "";
		for (int i = 0; i < inputFileNames.length; i++) {
			try {
			bufferedReader = new BufferedReader(new FileReader(inputFileNames[i]));
			line = bufferedReader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			String[] newColumns = line.split(",");
			for (int j = 0; j < newColumns.length; j++) {
				if (!columns.containsKey(newColumns[j])) {
					columns.put(newColumns[j],columns.size());
				}
			}
			try {
				bufferedReader.close();				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return columns;
	}
	
	/**
	 * Accepts a BufferedWriter and writes a batch of lines to the BufferedWriter
	 * @param outputFileBuffer A BufferedWritter that will be closed by the function that calls this function
	 * @param lines An array of lines that will be written to the file
	 */
	public void writeLinesToFile(BufferedWriter outputFileBuffer,String[] lines) {
		try {
			String line;
			int index = 0;
			do {
				line = lines[index];
				if (line != null) {
					outputFileBuffer.write(line);
					outputFileBuffer.newLine();
				}
				index ++;
			} while (line != "" && index < lines.length);
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void handleFirstLine(String line) {
		String[] fileColumns = line.split(",");
		this.currFileColumnPlacement = new int[fileColumns.length];
		for (int k = 0; k < fileColumns.length; k++) {
			this.currFileColumnPlacement[k] = columns.get(fileColumns[k]);
		}
	}
	
	private String[] bracketCombine(String[] arrOfStrings) {
		int openBracket = -1;
		for (int i = 0; i < arrOfStrings.length; i++) {
			if (openBracket != -1 && arrOfStrings[i].matches("(.*)\\](.*)")) {
				arrOfStrings[openBracket] =  arrOfStrings[openBracket] + "\",\"" + arrOfStrings[i];
				arrOfStrings[i] = "";
				openBracket = -1;
			}
			if (openBracket != -1) {
				arrOfStrings[openBracket] =  arrOfStrings[openBracket] + "\",\"" + arrOfStrings[i];
				arrOfStrings[i] = "";
			}
			if (arrOfStrings[i].matches("(.*)\\[(.*)") && !arrOfStrings[i].matches("(.*):\\[") && !arrOfStrings[i].matches("(.*)\\](.*)")) {
				openBracket = i;
			}
			
		}
		ArrayList<String> combined = new ArrayList<>();
		for (int i = 0; i<arrOfStrings.length;i++) {
			if (arrOfStrings[i] != "") {
				combined.add(arrOfStrings[i]);
			}
		}
		return combined.toArray(new String[0]);
	}
}
