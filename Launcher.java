// NOTE: YOU! YES, YOU! When running the Docker container for this code run the following command!!!!
// sudo docker run -d --network="host" --rm -p 8082:8082 cs505-final

package cs505-final-project;

import cs505-final-project.CEP.CEPEngine;
import cs505-final-project.Topics.TopicConnector;
import cs505-final-project.graphDB.GraphDBEngine;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class Launcher {

    public static GraphDBEngine graphDBEngine;
    public static String inputStreamName;
    public static CEPEngine cepEngine;
    public static TopicConnector topicConnector;
    public static final int WEB_PORT = 8082;

    // Map for zipcodes output from CEP
    public static Map<String,Integer> old_zipcodes = new HashMap<>();
    public static Map<String,Integer> zipcodes = new HashMap<>();
    // List of alerted zipcodes
    public static ArrayList<String> alert_zips = new ArrayList<String>();

    public static String lastCEPOutput = "{}";

    public static void main(String[] args) throws IOException {


        //startig DB/CEP init

        //READ CLASS COMMENTS BEFORE USING
        //graphDBEngine = new GraphDBEngine();
        
        //New instance of CEPEngine
        cepEngine = new CEPEngine();

        System.out.println("Starting CEP...");
        
        //Input stream has one attribute - zip_code, of type string
        inputStreamName = "testInStream";
        String inputStreamAttributesString = "zip_code string";

        //Output stream has two attributes - zip_code of type string, count of zip codes type long
        String outputStreamName = "testOutStream";
        String outputStreamAttributesString = "zip_code string, count long";
        
        //RTR 1 - Changed the query string from template, may need refactor
        //This query must be modified.  Currently, it provides the last zip_code and total count
        //You want counts per zip_code, to say another way "grouped by" zip_code
        String queryString = " " +
                "from testInStream#window.timeBatch(15 sec) " +
                "select zip_code, count(zip_code) as count " +
                "group by zip_code " +
                "insert into testOutStream; ";

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);

        System.out.println("CEP Started...");
        //end DB/CEP Init

        //start message collector
        //Los Jalapenos info
        Map<String,String> message_config = new HashMap<>();
        message_config.put("hostname","128.163.202.50");
        message_config.put("username","student");
        message_config.put("password","student01");
        message_config.put("virtualhost","7");

        topicConnector = new TopicConnector(message_config);
        topicConnector.connect();
        //end message collector

        //Embedded HTTP initialization
        startServer();

        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("cs505-final-project.httpcontrollers");

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
