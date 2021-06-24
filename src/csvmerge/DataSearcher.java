package csvmerge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.regex.Pattern;

/**
 * A class used to maintain and arrange the data searches done on a csv file
 * @author NSriram
 *
 */
public class DataSearcher {
    private String searchFile;
    private HashMap<String,Integer> columns;
    private int[] columnAssignments;
    private HashMap<Integer,DataSearch> searchCache; 

    public DataSearcher(String searchFile, HashMap<String,Integer> columns) {
        this.searchFile = searchFile;
        this.columns = columns;
        columnAssignments = new int[this.columns.size()];
        for (int i = 0; i<columnAssignments.length;i++) {
            columnAssignments[i] = i;
        }
        this.searchCache = new HashMap<>();

    }

    /**
     * Adds a search to the DataSearcher object and returns the DataSearch object produced. Uses a cached object if available
     * @param searchTerm
     * @return
     */
    public DataSearch addSearch(String searchTerm) {
        String[] empty = {};
        return addSearch(searchTerm, empty, false);
    }

    /**
     * Adds a search to the DataSearcher object and returns the DataSearch object produced. Uses a cached object if available
     * @param searchTerm
     * @param searchColumns
     * @return
     */
    public DataSearch addSearch(String searchTerm, String[] searchColumns) {
        return addSearch(searchTerm, searchColumns, false);
    }

    /**
     * Adds a search to the DataSearcher object and returns the DataSearch object produced. Uses a cached object if available
     * @param searchTerm
     * @param searchColumns
     * @param useRegEx
     * @return
     */
    public DataSearch addSearch(String searchTerm, String[] searchColumns, boolean useRegEx) {
        DataSearch newSearch = new DataSearch(searchTerm, searchColumns, useRegEx);
        if (this.searchCache.containsKey(newSearch.hashCode())) {
            DataSearch cachedSearch = this.searchCache.get(newSearch.hashCode());
            cachedSearch.updateNewDateTimeSearched();
            return cachedSearch;
        }
        Pattern regex = this.regexPatternMaker(searchTerm, useRegEx);
        HashSet<Integer> searchColumnIndices = this.searchColumnIndices(searchColumns);
        LinkedList<DataLine> dataLineList = new LinkedList<>();
        BufferedReader resultReader = null;
        int searchedLinesCount = 0;
        
        try{
            resultReader = new BufferedReader(new FileReader(searchFile));
        } catch (IOException e) {
            e.printStackTrace();
        }

        String line = "";

        try {
			line = resultReader.readLine();
			line = resultReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

        while (line != null){
			searchedLinesCount++;
            DataLine dataLine = new DataLine(this.columnAssignments, line, this.columnAssignments.length);
            while (!dataLine.splitTest()){
                String newSegment = "";
                try{
                    newSegment = resultReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                dataLine.addToRaw(newSegment);
            }
            try{
                dataLine.reassignToNewColumns();
            } catch (ExpectedColumnCountMismatch e) {
                e.printStackTrace();
            }
            if (searchMatch(regex, dataLine, searchColumnIndices)) {
                dataLineList.add(dataLine);
            }
            try {
    			line = resultReader.readLine();			
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
        }

        DataLine[] hits = dataLineList.toArray(new DataLine[0]);
        DataSearch results = new DataSearch(searchTerm, searchColumns, useRegEx, hits, searchedLinesCount);
        this.searchCache.put(newSearch.hashCode(),results);
        return results;
    }
    
    /**
     * Makes a flexible regex pattern based on the user parameters for the search
     * @param searchTerm
     * @param useRegEx
     * @return
     */
    private Pattern regexPatternMaker(String searchTerm, boolean useRegEx) {
        if (useRegEx) {
            return Pattern.compile(searchTerm);
            
        } else {
            return Pattern.compile("(.*)"+searchTerm+"(.*)",Pattern.CASE_INSENSITIVE);
        }
    }

    /**
     * Adds search column indices to a hashset for quick retrieval
     * @param searchColumns
     * @return
     */
    private HashSet<Integer> searchColumnIndices(String[] searchColumns) {
        HashSet<Integer> lookupColumns = new HashSet<>();
        for (int i = 0; i<searchColumns.length;i++) {
            lookupColumns.add(this.columns.get(searchColumns[i]));
        }
        return lookupColumns;
    }

    /**
     * A function to test if the dataline matches the conditions of the search
     * @param searchPattern
     * @param dataLine
     * @param searchColumnIndices
     * @return true if the dataline meets the requirements of the search
     */
    private boolean searchMatch(Pattern searchPattern, DataLine dataLine, HashSet searchColumnIndices) {
        String[] columnedData = dataLine.getColumnedData();
        for (int i = 0; i<columnedData.length;i++){
            boolean seachColumnsUnSpecified = (searchColumnIndices.size() == 0);
            if (seachColumnsUnSpecified) {
            	boolean patternMatch = searchPattern.matcher(columnedData[i]).matches();
            	if (patternMatch) {
            		return true;
            	}
            } else {
                boolean specifiedSearchColumn = searchColumnIndices.contains((Integer)i);
                boolean patternMatch = searchPattern.matcher(columnedData[i]).matches();
                if (specifiedSearchColumn && patternMatch) {
                    return true;
                }
            }
        }
        return false;
    }

}
