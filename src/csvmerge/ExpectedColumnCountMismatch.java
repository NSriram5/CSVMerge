package csvmerge;

/**
 * A class to manage exceptions having to do with number of columns not adding up
 * @author NSriram
 *
 */
public class ExpectedColumnCountMismatch extends Exception {
    
    public ExpectedColumnCountMismatch(String line) {
		super(line + " did not divide into the expected number of columns");
	}

    public ExpectedColumnCountMismatch(String[] line) {
		super(line + " did not divide into the expected number of columns");
	}
}
