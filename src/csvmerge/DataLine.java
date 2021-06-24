package csvmerge;

import java.util.ArrayList;
import java.util.Arrays;


/** 
 * A class to help organize and arrange the elements of a data line into columns
 * @author NSriram
 */
public class DataLine {
    private String rawLine;
    private String[] columnedLine;
    private int numberOfColumns;
    private int[] columnAssignments;
    private DataType[] columnTypes;
    private int completeNumberOfColumns;
    

    //Constructor for if the columnTypes are not known
    public DataLine(int[] columnAssignments, String rawLine, int completeNumberOfColumns) {
        this.completeNumberOfColumns = completeNumberOfColumns;
        this.numberOfColumns = columnAssignments.length;
        this.columnTypes = new DataType[this.numberOfColumns];
        Arrays.fill(this.columnTypes, DataType.UNKNOWN);
        this.columnAssignments = columnAssignments;
        this.rawLine = rawLine;
    }

    //Constructor for if the columnTypes are known well enough to validate against
    public DataLine(int[] columnAssignments,DataType[] columnTypes, String rawLine, int completeNumberOfColumns) {
        this.completeNumberOfColumns = completeNumberOfColumns;
        this.numberOfColumns = columnAssignments.length;
        this.columnAssignments = columnAssignments;
        this.columnTypes = columnTypes;
        this.rawLine = rawLine;
    }

    //Constructor for if the columns are already arranged and the
    public DataLine(String[] columnedLine, DataType[] columnTypes) {
        this.completeNumberOfColumns = columnedLine.length;
        this.numberOfColumns = this.completeNumberOfColumns;
        this.columnTypes = columnTypes;
        this.columnedLine = columnedLine;
    }

    /**
     * return false if there are not enough columns, and another line needs to be added
     * 1. Access this function first to determine if new raw text needs to be added
     * @return true if there are enough columns to treat this line of data as valid
     */
    public boolean splitTest() {
        if (!endswithquotes()) {
            return false;
        }

        String[] output = this.rawLine.split(",");
        output = fixCommaSplits(output, this.columnTypes);

        if (output.length != this.numberOfColumns) {
            return false;
        }
        this.columnedLine = output;
        return true;
    }

    /**
     * Add another segment to the raw line
     * 2. Access this function second if new text needs to be added
     * @param rawAddition a new segment to add to the raw line
     */
    public void addToRaw(String rawAddition) {
        this.rawLine = this.rawLine + rawAddition;
    }

    /**
     * return true if line's data items pass their data type tests or if the input column types were all UNKNOWN
     * 3. Run this function if the column datatypes need to be tested
     */
    public boolean testDataTypes() throws ExpectedColumnCountMismatch {
        if (this.columnedLine.length != this.columnTypes.length) {
            throw new ExpectedColumnCountMismatch(this.columnedLine);
        }
        for (int i = 0; i < this.columnedLine.length; i++) {
            if (this.columnTypes[i] != DataType.UNKNOWN) {
                if (!DataItem.testDataType(this.columnedLine[i], this.columnTypes[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * returns the column types for this dataline
     * 4. Run this function if the column datatypes are not mature enough to be tested
     * @return the guessed column types
     * @throws ExpectedColumnCountMismatch
     */
    public DataType[] guessDataTypes() throws ExpectedColumnCountMismatch {
        if (this.columnedLine.length != this.columnTypes.length) {
            throw new ExpectedColumnCountMismatch(this.columnedLine);
        }
        for (int i = 0; i < this.columnedLine.length; i++) {
            if (this.columnTypes[i] == DataType.UNKNOWN) {
                this.columnTypes[i] = DataItem.guessDataType(this.columnedLine[i]);
            }
        }
        return this.columnTypes;
    }

    /**
     * returns the dataline expanded into the right number of new columns at the right places
     * 5. Run this function to place the columns in the right place
     * @return an array of strings that matches the completeNumberOfColumns. All Empty columns are have "" (Open and closed double quotes)
     */
    public DataLine reassignToNewColumns() throws ExpectedColumnCountMismatch {
        String[] fullColumns = new String[this.completeNumberOfColumns];
        DataType[] fullDataTypeColumns = new DataType[this.completeNumberOfColumns];
        Arrays.fill(fullColumns,"\"\"");
        Arrays.fill(fullDataTypeColumns,DataType.UNKNOWN);
        for (int i = 0; i < columnedLine.length; i++) {
            if (columnAssignments[i] >= this.completeNumberOfColumns) {
                throw new ExpectedColumnCountMismatch("");
            }
            fullColumns[columnAssignments[i]] = this.columnedLine[i];
            fullDataTypeColumns[columnAssignments[i]] = this.columnTypes[i];
        }
        return new DataLine(fullColumns, fullDataTypeColumns);
    }

    /**
     * Outputs the dataline to a string so that it can be added to a text file
     * @return a string assembled from the contents of this dataline
     */
    public String toString() {
        String output = "";
        for (int i=0;i<columnedLine.length;i++) {
            if (i == 0) {
                output = columnedLine[i];
                continue;
            }
            output = output + "," + columnedLine[i];
        }
        return output;
    }

    public String[] getColumnedData() {
        return this.columnedLine;
    }

    public String getRawLine() {
        return this.rawLine;
    }

//******************************************************* Private methods */

    /**
     * Attempt to fix any columns that were errorneously broken with a naive split on commas only. Return the attempted columnization of data
     * @param initialSplit the initial naive split of data
     * @param columnTypes hypothesis of column types
     * @return
     */
    private String[] fixCommaSplits(String[] initialSplit, DataType[] columnTypes) {
        //recombine items that were incorrectly separated
        int column = 0;
        int limit = initialSplit.length;
        for (int i = 0; i < initialSplit.length; i++ ) {
            int j = i+1;
            //If this split has already been recombined move on.
            if (initialSplit[i].length()==0) {
                continue;
            }
            while (DataItem.combineWithNext(initialSplit[i], columnTypes[column]) && j < limit) {
                initialSplit[i] = initialSplit[i] + "," + initialSplit[j];
                initialSplit[j] = "";
                j++;
            }
            column++;
        }

        ArrayList<String> recombiner = new ArrayList<>();
        for (int i = 0; i < initialSplit.length; i++ ) {
            if (initialSplit[i].length() > 0) {
                recombiner.add(initialSplit[i]);
            }
        }
        return recombiner.toArray(new String[0]);
    }


    /**
     * returns true if the raw line ends with quotes. A failure can indicate that another line needs to be read
     * @return if a new line probably needs to be read
     */
    private boolean endswithquotes() {
        return this.rawLine.endsWith("\"");
    }
    
    

}