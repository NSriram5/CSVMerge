package csvmerge;

import java.util.zip.ZipInputStream;
//import java.util.zip.ZipException;
//import java.util.zip.DataFormatException;
import java.util.zip.ZipEntry;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;


/**
 * A simple utility class which can unZip a zip compressed file to a destination
 * @author NSriram
 *
 */
public class Unzipper {

	/**
	 * Unzips a given zip archive to a given filepath destination
	 * @param zipName the directory path to the zip archive
	 * @param destination the path to the folder for the destined items
	 * @throws IOException
	 */
	public static void unZip(String zipName,String destination) throws IOException {
		File destinationDir = new File(destination);
		byte[] buffer = new byte[1024];
		ZipInputStream zipStream = new ZipInputStream(new FileInputStream(zipName));
		ZipEntry zipItem = zipStream.getNextEntry();
		while (zipItem != null) {
			File newFile = new File(destinationDir,zipItem.getName());
			if (zipItem.isDirectory()) {
				if (!newFile.isDirectory() && !newFile.mkdirs()) {
					throw new IOException("The process has failed to make a directory " + newFile);
				}
			} else {
				File parent = newFile.getParentFile();
				if (!parent.isDirectory() && !parent.mkdirs()) {
					throw new IOException("Failed to create directory" + parent);
				}
				FileOutputStream fileOutStream = new FileOutputStream(newFile);
				int length;
				while (( length = zipStream.read(buffer)) > 0) {
					fileOutStream.write(buffer,0,length);
				}
				fileOutStream.close();
			}
			zipStream.closeEntry();
			zipItem = zipStream.getNextEntry();
		}
		try {
			if (zipStream != null) {
				zipStream.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
