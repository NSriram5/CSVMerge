/**
 * 
 */
package csvmerge;

/**
 * A class to manage exceptions having to do with primary key choice
 * @author NSriram
 *
 */
public class ColumnSeparationMismatch extends Exception {
	private static final long serialVersionUID = -8023433378397167262L;

	public ColumnSeparationMismatch(String line) {
		super("Attempted read of line produced mismatch with number of columns: " + line);
	}
}
