package csvmerge;

import java.time.LocalDateTime;
import java.util.LinkedList;

/**
 * A class used to hold both the search parameters and the search parameters with their results. Overrides the hashCode  
 * @author artain
 *
 */
public class DataSearch {
    private String[] searchColumns;
    private String searchTerm;
    private boolean useRegEx;
    private DataSearchResults results;
    

    public DataSearch(String searchTerm, String[] searchColumns, boolean useRegEx) {
        this.searchTerm = searchTerm;
        this.searchColumns = searchColumns;
        this.useRegEx = useRegEx;
        this.results = null;
    }

    public DataSearch(String searchTerm, String[] searchColumns, boolean useRegEx, int count, int outOfNumber) {
        this.searchTerm = searchTerm;
        this.searchColumns = searchColumns;
        this.useRegEx = useRegEx;
        this.results = new DataSearchResults(count, LocalDateTime.now(), outOfNumber);
    }

    public DataSearch(String searchTerm, String[] searchColumns, boolean useRegEx, DataLine[] hits, int outOfNumber) {
        this.searchTerm = searchTerm;
        this.searchColumns = searchColumns;
        this.useRegEx = useRegEx;
        this.results = new DataSearchResults(hits, hits.length, LocalDateTime.now(), outOfNumber);
    }

    public String getSearchTerm() {
        return this.searchTerm;
    }

    public String[] getSearchColumns() {
        return this.searchColumns;
    }

    public boolean usedRegEx() {
        return this.useRegEx;
    }

    public DataSearchResults getResults(){
        try{ 
            return this.results;
        } catch (NullPointerException e) {
            return null;
        }
    }

    public LocalDateTime getLastTimeSearched() {
        try {
            return this.results.getdateTime();
        } catch (NullPointerException e) {
            return null;
        }
    }

    public void updateNewDateTimeSearched() {
        try {
            this.results.updateDateTime(LocalDateTime.now());
        } catch (NullPointerException e) {
            return;
        }
    }

    /**
     * A hashcode function to help evaluate data searches against one another
     */
    @Override
    public int hashCode() {
        int hash = 7;
        for (String column: this.searchColumns) {
        	hash = 17 * hash + column.hashCode();
        }
        hash = 37 * hash + this.searchTerm.hashCode();
        hash = 79 * hash + (this.useRegEx ? 1 : 0);
        return hash;
    }
    
    /**
     * Produces a string that can be printed or written to a file displaying the results
     * @return a string of the results of this search
     */
    public String outcomeToString() {
    	DataSearchResults myresults;
    	try {
    		myresults = this.results;
    	} catch (NullPointerException e) {
    		return "Search has not yet been conducted";
    	}
    	String output = "Search Results: \n";
    	output += "Searched for " + this.searchTerm + "\n";
    	if (this.searchColumns.length>0) {
    		output += "In columns: ";
    		for (int i=0;i<this.searchColumns.length-1;i++) {
    			output+= this.searchColumns[i] + ", ";
    		}
    		output += this.searchColumns[this.searchColumns.length-1] + "\n";
    	}
    	if (this.useRegEx) {
    		output += "Using regex \n";
    	}
    	output += "Last Searched on :" + myresults.getdateTime().toString() + "\n";
    	LinkedList<?> searchHistory = myresults.getSearchFrequency();
    	if (searchHistory.size()>1) {
    		output += "Searched before at the following dates and times: \n";
    		for (Object item: searchHistory) {
    			output += item.toString() + "\n";
    		}
    	}
    	output += "Search returned " + myresults.getHitCount() + " hits \n";
    	output += "Out of " + myresults.getOutOfNumber() + " records searched\n";
    	output += "Results were:\n";
    	DataLine[] hits = myresults.getHits();
    	for (int i = 0; i<hits.length && i<25;i++) {
    		output += hits[i].toString() + "\n";
    	}
    	if (hits.length > 25) {
    		output += "[Results Truncated]\n";
    	}
    	return output;
    }

}
