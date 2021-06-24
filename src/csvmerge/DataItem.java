package csvmerge;

public class DataItem {
    
    
    public static DataType guessDataType(String input) {
        if (validNumber(input)) {
            return DataType.NUMBER;
        } else if (valid47DigitIdentifier(input)) {
            return DataType.IDENTIFIER7DIGIT;
        } else if (guessDateTime(input)) {
            return DataType.DATETIME;
        } else if (guessArray(input)) {
            return DataType.ARRAY;
        } else if (validResourceAddress(input)) {
            return DataType.RESOURCEADDRESS;
        } else if (guessBoolean(input)) {
        	return DataType.BOOLEAN;
    	} else if (guessString(input)) {
            return DataType.STRING;
        }
        return DataType.UNKNOWN;
    }

    public static boolean testDataType(String input, DataType type) {
        switch (type) {
            case IDENTIFIER7DIGIT:
                return valid47DigitIdentifier(input);
            case NUMBER:
                return validNumber(input);
            case DATETIME:
                return validDateTime(input);
            case ARRAY:
                return validArray(input);
            case RESOURCEADDRESS:
                return validResourceAddress(input);
            case BOOLEAN:
            	return validBoolean(input);
            case STRING:
                return validString(input);    
            default:
                return false;
        }
    }

    public static boolean combineWithNext(String input, DataType columnType) {
        if (columnType != DataType.UNKNOWN) {
            return !testDataType(input, columnType);
        }
        //String is the most lenient standard. If it can't pass the string test then there's probably a part missing.
        return !testDataType(input, DataType.STRING);
    }

    private static boolean valid47DigitIdentifier(String input) {
        return input.matches("^\"\\d{4,7}\"$");
    }

    private static boolean validNumber(String input) {
        return input.matches("^\"-?[,\\d]*\\.?\\d*\"$");
    }

    private static boolean guessDateTime(String input) {
    	return input.matches("^\"\\d{4}-\\d{2}-\\d{2}T.*\"$");
    }
    
    private static boolean validDateTime(String input) {
        return input.matches("(^\"\\d{4}-\\d{2}-\\d{2}T.*\"$)|(^\"null\"$)");
    }
    
    private static boolean guessArray(String input) {
    	return input.matches("^\"\\[\"\"(.*)\"\"$");
    }
    
    private static boolean validArray(String input) {
        return input.matches("^\"\\[\"\"(.*)\"\"\\]\"$|(^\"null\"$)");
    }    

    private static boolean validResourceAddress(String input) {
        return input.matches("^\"(/)|(http)[^\\s]*\"$");
    }
    private static boolean guessString(String input) {
    	return input.matches("^\".*");
    }
    
    private static boolean validString(String input) {
    	int quoteCount = 0;
    	char quote = "\"".charAt(0);
    	for (int i = 0; i<input.length();i++) {
    		if (input.charAt(i) == quote) {
    			quoteCount++;
    		}
    	}
    	if (quoteCount%2 != 0) {
    		return false;
    	}
        return input.matches("^\".*\"$");
    }
    
    private static boolean validBoolean(String input) {
    	return input.matches("^\"[(true)(false)(null)]\"$");
    }
    
    private static boolean guessBoolean(String input) {
    	return input.matches("^\"(true)|(false)\"$");
    }
}
