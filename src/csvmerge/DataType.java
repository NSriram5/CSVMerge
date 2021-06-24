package csvmerge;

public enum DataType {
    NUMBER, 

    //Strings can have commas

    STRING, 

    //Arrays have matching square brackets and comma separations
    //Items in arrays are closed in double quotes
    ARRAY, 

    //Used as a primary key and have 7 numerical digits
    IDENTIFIER7DIGIT, 

    //resource delimited with forward slashes and can have commas. No spaces
    RESOURCEADDRESS, 
    DATETIME, 
    BOOLEAN,
    UNKNOWN
}
