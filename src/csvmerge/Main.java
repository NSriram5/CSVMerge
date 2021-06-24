package csvmerge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class Main {

	public static String[] unzip(String[] args) {
		String[] zipPaths;
		String startingPath;
		String unZipPath;
		String mergedDestination;
		ArrayList<String> dataPathsList = new ArrayList<>();
		if (args.length != 0) {
			startingPath = args[0];
			unZipPath = args[1];
			mergedDestination = args[2];
		} else {
			startingPath = "../indiegogo-dataset";
			unZipPath = "../unzipped";
			mergedDestination = "../merged.csv";
		}
		File f = new File(startingPath);
		zipPaths = f.list();
		
		int i = 0;
		
		for (String path : zipPaths) {
			try {
				String sourcePath = startingPath + "/" + path;
				String destinationPath = unZipPath + String.valueOf(i);
				System.out.println("unzipping:" + sourcePath);
				Unzipper.unZip(sourcePath, destinationPath);
				File g = new File(destinationPath);
				String[] unzippedPaths = g.list();
				for (String unzippedPath: unzippedPaths) {
					//System.out.println("produced: " + destinationPath + "/" + unzippedPath);
					dataPathsList.add(destinationPath + "/" + unzippedPath);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			i++;
		}
		System.out.println("Unzipping complete");
		return dataPathsList.toArray(new String[0]);
	}
	
	public static void main(String[] args) {
		String mergedDestination;
		if (args.length != 0) {
			mergedDestination = args[2];
		} else {
			mergedDestination = "../merged.csv";
		}
		String[] csvDataFiles = Main.unzip(args);
		DataFramer dataFramer = null;
		try {
			dataFramer = new DataFramer(csvDataFiles, mergedDestination);			
		} catch (PrimaryKeyNotFoundException e) {
			e.printStackTrace();
		}
		dataFramer.mergeCSVFiles();
		DataSearcher myFinder = dataFramer.getDataSearcher();
		DataSearch search = myFinder.addSearch("comics");
		System.out.println(search.outcomeToString());
		
		search = myFinder.addSearch("comics");
		System.out.println(search.outcomeToString());
	}

}
