## Read and Merge CSV files
This assignment was intended to demonstrate competency with Collections and generics.

To organize this project I divided the code into a module that would help me unzip batches of file, a module that would help frame data into a merged dataset. A class that could arrange data into a coherent line, and regex schema logic class to help test different column data types. As a new feature, a data searching tool was added including a data search object and data search result object.

To guide my process I initially created a flow diagram of the control sequence within the software. Although this diagram evolved with the development of the code, it provided an organizing backbone to the code.

As an update from the previous version, I implemented a rudimentary guessing process by which the software could make an attempt at framing different data into datatype categories. Once established, these datatype categories help the merging process chew through a variety of different forms of data with greater ease. The logic for the testing and analysis of these different datatypes is drawn into it's own class component where it can easily be modified or added to based on the need of future requirements.

To further compartmentalize the data and functions of this task, I created a dataline object which is designed to work on one line of data at a time. Later on this is useful when searching a output file for desired search terms.

Overall, I attempted to make the merging process more resilient to variations in input data.

As far as searching goes, once a file merge is complete, the client has access to a finder object, which can be used as a constructor for searches. These searches are stored in HashMap collection with the keys being a hashcode of search parameters and the values being the stored data results. If a search term is searched again in succession, the cache makes a note of the latest time that the search was made and the sequential date and time are stored in a linkedlist.

During a new search, lines from the merged file are read, and parsed in a similar way to the merging process. When a dataline meets the necessary conditions of a search, it is added to a linked list within the loop. At the end of the reading loop, the linked list is then converted to an array, since additions and removals from the search results are no longer needed.

Overall, this implementation made use of HashMaps, and LinkedList for collections. The Hashmap was to check for the existence of previous executions of the same search. LinkedLists were used to rapidly add or provide a readable format of information. An arraylist was not chosen because the time complexity advantages of adding or removing an element at an arbitrary index was not needed, and linked list provided a slight advantage when appending new elements.

A generic wildcard was used in one place as a matter of demonstration. When the list of datetimes is printed to a string, the LinkedList of dateTimes is structured to accept a wildcard generic as a storage parameter. This could allow different types of objects to be stored within the date time search-frequency data record.

A brief UML of the system designed is shown below:
![UML of the CSV Merge and Search](/BUMETCS622-HW3.jpeg)
