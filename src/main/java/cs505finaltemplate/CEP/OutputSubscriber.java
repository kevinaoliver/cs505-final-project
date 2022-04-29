package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            //System.out.println("OUTPUT CEP EVENT: " + msg);
            //System.out.println("");

            // Clear the map of zipcodes before sending a new batch to it
            Launcher.zipcodes.clear();

            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
            String[] split_msg = String.valueOf(msg).split(",");
            System.out.println("OUTPUT CEP EVENT: " );
           
            String zipcode = "";
            String count_str = "";
            int count = 0;

            for (int i = 0; i < split_msg.length; i++) {
                //System.out.println(split_msg[i]);

                // Search for number 4 in string. This is the beginning number of all the zipcodes.
                // If the string doesn't contain a 4, then it is the count string
                if (split_msg[i].contains("4")) {
                    // Grab the index of "4"
                    int index = split_msg[i].indexOf("4");

                    // Grab the substring containing the zipcode
                    // Goes to index + 5 instead of index + 4 because substring function does not include the character at the end index
                    zipcode = split_msg[i].substring(index, index + 5);
                } else { // The string does not contain a zipcode, it contains the count instead
                    
                    // Iterate through string to get entire count number by adding it to count string
                    for (int j = 0; j < split_msg[i].length(); j++) {
                        // Accesses char at index j, check if it's a digit 
                        char c = split_msg[i].charAt(j);
                        if(Character.isDigit(c)) {
                            // Concatenate string if it's a digit
                            count_str = count_str + c;
                        }
                    } // end for loop
                    
                    // Change count from string to int
                    count = Integer.parseInt(count_str);
                    // Reset count_str 
                    count_str = "";

                    // Add zipcode and count to zipcode map from Launcher
                    Launcher.zipcodes.put(zipcode,count);
                } // end else statement
            } // end for loop

            // DEBUG: Print zipcodes map from Launcher
            System.out.println(Launcher.zipcodes);

            //String[] sstr = String.valueOf(msg).split(":");
            //String[] outval = sstr[2].split("}");
            //Launcher.accessCount = Long.parseLong(outval[0]);

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
