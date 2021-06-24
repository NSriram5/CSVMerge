/**
 * 
 */
package csvmerge;

/**
 * A class to manage exceptions having to do with primary key choice
 * @author NSriram
 *
 */
public class PrimaryKeyNotFoundException extends Exception {
	private static final long serialVersionUID = -8023433378397167262L;

	public PrimaryKeyNotFoundException(String attemptedKey) {
		super(attemptedKey + " was chosen as a primary key but isn't a column in all "
				+ "of the data");
	}

	
}
