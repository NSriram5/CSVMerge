package csvmerge;

import java.time.LocalDateTime;
import java.util.LinkedList;

/**
 * A data object used to store the results of a data search
 * @author NSriram
 *
 */
public class DataSearchResults {
    private DataLine[] hits;
    private int resultCount;
    private int outOfNumber;
    private LinkedList<LocalDateTime> dateTime;

    /**
     * Constructor for if the results are too many to be feasibly stored
     * @param resultCount
     * @param dateTime
     * @param searched
     */
    public DataSearchResults(int resultCount, LocalDateTime dateTime, int searched) {
        this.resultCount = resultCount;
        this.outOfNumber = searched;
        this.dateTime = new LinkedList<>();
        this.dateTime.add(dateTime);
        this.hits = new DataLine[0];
    }
    
    /**
     * Constructor for the typical use case. When results can be stored completely in an array
     * @param hits
     * @param resultCount
     * @param dateTime
     * @param searched
     */
    public DataSearchResults(DataLine[] hits, int resultCount, LocalDateTime dateTime, int searched) {
        this.hits = hits;
        this.outOfNumber = searched;
        this.resultCount = resultCount;
        this.dateTime = new LinkedList<>();
        this.dateTime.add(dateTime);
    }

    /**
     * Produces the count of the hits that met the parameters of this search
     */
    public int getHitCount() {
        return this.resultCount;
    }
    
    /**
     * Returns the count of other lines were read to produce these results
     */
    public int getOutOfNumber() {
    	return this.outOfNumber;
    }

    /**
     * Produces the array of hits that satisfied this search
     */
    public DataLine[] getHits() {
        return this.hits;
    }

    /**
     * produces the last date and time that this search result was requested
     * @return the data and time this result was last requested
     */
    public LocalDateTime getdateTime() {
        return this.dateTime.getLast();
    }
    
    public LinkedList<?> getSearchFrequency() {
    	return this.dateTime;
    }

    /**
     * Updates the last date and time that this search result was requested
     * @param dateTime
     */
    public void updateDateTime(LocalDateTime dateTime) {
        this.dateTime.add(dateTime);
    }
}
