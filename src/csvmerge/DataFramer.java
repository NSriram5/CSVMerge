package csvmerge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import java.util.HashMap;

public class DataFramer {
    private static final int BATCHLENGTH = 100;
    private static final String PRIMARYKEY = "project_id";
    private static final String[] PRIMARYKEY_ALIASES = {"project_id","id","ID","Id","Project_id","PROJECT_ID","project_Id"};
    private static final String[] ARRAY_COLUMNS = {"tags"};
    
    private static final String DUPLICATE_REPORT_PATH = "duplicatesFound.csv";
    private static final String ERROR_REPORT_PATH = "errors.csv";
    private HashSet<String> duplicateLookup = new HashSet<>();
    private BufferedReader csvReader = null;
    private BufferedWriter csvWriter = null;
    private BufferedWriter duplicateWriter = null;
    private BufferedWriter errorWriter = null;
    
    private DataSearcher finder;

    private String[] inputFileNames;
    private String outputFilePath;
    private HashMap<String,Integer> columns = null;
    private int primaryKeyColumn;
    private boolean mergeComplete;

    public DataFramer(String[] inputFileNames, String outputFilePath) throws PrimaryKeyNotFoundException {
        this.inputFileNames = inputFileNames;
        this.outputFilePath = outputFilePath;
        this.columns = this.buildColumnMap(inputFileNames);
        this.mergeComplete = false;
        this.findPrimaryKeyColumn();
        this.finder = new DataSearcher(this.outputFilePath, this.columns);
    }

    /**
     * A time costly operation which merges all of the files in the inputFileNames array into the designated output filepath array.
     */
    public void mergeCSVFiles() {
        if (mergeComplete) {
            return;
        }
        try{
            this.prepFiles();
            this.csvWriter = new BufferedWriter(new FileWriter(this.outputFilePath,true));
            this.duplicateWriter = new BufferedWriter(new FileWriter(DUPLICATE_REPORT_PATH,true));
            this.errorWriter = new BufferedWriter(new FileWriter(ERROR_REPORT_PATH,true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.writeHeaderRow();

        for (int i = 0; i < inputFileNames.length; i++) {
			System.out.println("Reading file: " + inputFileNames[i]);
            try {
                this.readFileIntoOutputFile(inputFileNames[i]);
            } catch(IOException e) {
                e.printStackTrace();
            }
		}
		try {
			csvWriter.close();
			duplicateWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        this.mergeComplete = true;
        this.finder = new DataSearcher(this.outputFilePath, this.columns);
    }

    /**
     * Returns the DataFramer's DataSearcher object so that searches can be performed on the merged file 
     * @return the DataSearcher object
     */
    public DataSearcher getDataSearcher() {
        return this.finder;
    }

    /**
     * Reads a file from an input filename string and adds it to the merged file output
     * @param inputFileName the input file name to read
     */
    private void readFileIntoOutputFile(String inputFileName) throws IOException {
        try{
            this.csvReader = new BufferedReader(new FileReader(inputFileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int currWriteLine = 0;
        String line = "";
        
		line = this.csvReader.readLine();			
		
		int[] columnAssignments = handleFirstLine(line);
		DataType[] dataTypeAssignments = prePopulateDataTypeArray(line);
        //DataType[] dataTypeAssignments = new DataType[columnAssignments.length];
        //Arrays.fill(dataTypeAssignments,DataType.UNKNOWN);
        int datatypeassignmentMaturity = 0;
		try {
			line = this.csvReader.readLine();			
		} catch (IOException e) {
			e.printStackTrace();
		}
        while (line != null) {
            String[] lines = new String[BATCHLENGTH];
            while (currWriteLine < BATCHLENGTH && line != null) {
                DataLine dataLine = null;
                dataLine = new DataLine(columnAssignments,dataTypeAssignments,line,columns.size());
                while (!dataLine.splitTest()) {
                    String newSegment = "";
                    newSegment = this.csvReader.readLine();
                    dataLine.addToRaw(newSegment);
                }
                try{
                    if (datatypeassignmentMaturity == 0 ) {
                        dataTypeAssignments = dataLine.guessDataTypes();
                        datatypeassignmentMaturity++;
                    } else if (datatypeassignmentMaturity < 6 ) {
                        if(!dataLine.testDataTypes()) {
                            dataTypeAssignments = dataLine.guessDataTypes();
                            datatypeassignmentMaturity = 1;
                        } else {
                            datatypeassignmentMaturity++;
                        }
                        datatypeassignmentMaturity++;
                    } else {
                        if(!dataLine.testDataTypes()) {
                            this.errorWriter.write("A line of data doesn't agree with the data types defined. Skipping");
                            this.errorWriter.newLine();
                            this.errorWriter.write(dataLine.getRawLine());
                            this.errorWriter.newLine();
                            this.errorWriter.write("Data types established:");
                            this.errorWriter.newLine();
                            this.errorWriter.write(dataTypeAssignments.toString());
                            this.errorWriter.newLine();
                            this.errorWriter.write("Datatype maturity for this file was:" + datatypeassignmentMaturity);
                            line = this.csvReader.readLine();
                            continue;
                        } else {
                            datatypeassignmentMaturity++;
                        }
                    }
                    dataLine.reassignToNewColumns();
                    
                } catch (ExpectedColumnCountMismatch e) {
                    this.errorWriter.write(dataLine.getRawLine());
                    this.errorWriter.newLine();
                    this.errorWriter.write(e.toString());
                    this.errorWriter.newLine();
                }
                if (testForDataDuplicate(dataLine.getColumnedData())) {
                    this.duplicateWriter.write(dataLine.toString());
                    this.duplicateWriter.newLine();
                } else {
                    lines[currWriteLine] = dataLine.toString();
                    duplicateLookup.add(dataLine.getColumnedData()[this.primaryKeyColumn]);
                    currWriteLine++;
                }

                line = this.csvReader.readLine();
            }
            System.out.println("Batch reading is complete writing lines to merge file");
            this.writeLinesToFile(this.csvWriter,lines);
            currWriteLine = 0;
        }
        csvReader.close();
    }

    /**
     * A helper function that gets files ready that will be written to
     * @throws IOException
     */
    private void prepFiles() throws IOException{
        FileWriter f1 = new FileWriter(this.outputFilePath,false);
        f1.close();
        f1 = new FileWriter(DUPLICATE_REPORT_PATH,false);
        f1.close();
        f1 = new FileWriter(ERROR_REPORT_PATH,false);
        f1.close();
    }

    /**
     * Builds the header row for the output file
     */
    private void writeHeaderRow() {
        StringBuilder headerRow = new StringBuilder();
        for (String key: columns.keySet()) {
            headerRow.append(key+",");
        }
        headerRow.deleteCharAt(headerRow.length()-1);
		try{
			this.csvWriter.write(headerRow.toString());
			this.duplicateWriter.write(headerRow.toString());
			csvWriter.newLine();
			duplicateWriter.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    /**
     * Handles the first line in the csv file
     * @param line
     */
    private int[] handleFirstLine(String line) {
		String[] fileColumns = line.split(",");
		int[] currFileColumnPlacement = new int[fileColumns.length];
		for (int k = 0; k < fileColumns.length; k++) {
            if (matchAnAliasPrimaryKey(fileColumns[k])) {
                fileColumns[k] = PRIMARYKEY;
            }
			currFileColumnPlacement[k] = columns.get(fileColumns[k]);
		}

        return currFileColumnPlacement;
	}
    
    /**
     * Handles the first line and returns some expected datatypes
     * @param line
     */
    public DataType[] prePopulateDataTypeArray(String line) {
    	String[] columnHeaders = line.split(",");
    	DataType[] dataTypes = new DataType[columnHeaders.length];
    	for (int k = 0; k < columnHeaders.length; k++) {
    		if (testForKnownColumnDataType(columnHeaders[k],ARRAY_COLUMNS)) {
    			dataTypes[k] = DataType.ARRAY;
    		} else if (testForKnownColumnDataType(columnHeaders[k],PRIMARYKEY_ALIASES)) {
    			dataTypes[k] = DataType.IDENTIFIER7DIGIT;
    		} else {
    			dataTypes[k] = DataType.UNKNOWN;
    		}
    	}
    	return dataTypes;
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
            //The column headers should all be simple text blocks with no spurious commas
			String[] newColumns = line.split(",");
			for (int j = 0; j < newColumns.length; j++) {
                //If this file has an alias for a primary key then convert it to a primary key
                if (this.matchAnAliasPrimaryKey(newColumns[j])) {
                    newColumns[j] = PRIMARYKEY;
                }
                //If the master columns map doesn't have this column header then add it
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
	 * A helper process that populates an pointer list of which columns are primary key columns
	 * @throws PrimaryKeyNotFoundException
	 */
	private void findPrimaryKeyColumn() throws PrimaryKeyNotFoundException {
		if (!columns.containsKey(PRIMARYKEY)) {
			throw new PrimaryKeyNotFoundException(PRIMARYKEY);
		}
		this.primaryKeyColumn = columns.get(PRIMARYKEY);
	}

    /**
     * A helper method that reports if a string is an alias for the primary key
     * @param header the column head to compare against the primary key aliases
     * @return true if the header provided is a match for a primary key alias
     */
    private boolean matchAnAliasPrimaryKey(String header) {
        for (int i = 0; i < PRIMARYKEY_ALIASES.length; i++) {
            if (header == PRIMARYKEY_ALIASES[i]) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines if a data types assignment array is brand new and only has unknowns
     * @param assignments
     * @return true if this is a brand new datatype assignment
     */
    private boolean newDataTypeAssignments(DataType[] assignments) {
        for (int i = 0; i<assignments.length; i++){
            if (assignments[i] != DataType.UNKNOWN) {
                return false;
            }
        }
        return true;
    }

    /**
	 * Accepts a BufferedWriter and writes a batch of lines to the BufferedWriter
	 * @param outputFileBuffer A BufferedWritter that will be closed by the function that calls this function
	 * @param lines An array of lines that will be written to the file
	 */
	private void writeLinesToFile(BufferedWriter outputFileBuffer,String[] lines) {
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

    /**
     * Tests an array of columnized string data to see if it's primary key matches an existing primary key 
     * @param data the columnized data
     * @return true if the data already exists in the merged data
     */
    private boolean testForDataDuplicate(String[] data) {
        if (duplicateLookup.contains(data[this.primaryKeyColumn])) {
            return true;
        }
        return false;
    }
    
    /**
     * Check to see if a column name matches some known column names to apply a datatype
     * @param columnName
     * @param columnNames
     * @return
     */
    private boolean testForKnownColumnDataType(String columnName, String[] columnNames) {
    	for (int i = 0; i<columnNames.length;i++) {
    		if(columnName.equals(columnNames[i])) {
    			return true;
    		}
    	}
    	return false;
    }

}
