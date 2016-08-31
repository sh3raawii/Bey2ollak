import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Mostafa on 8/31/16.
 */
public class LogParser {
    private String logFilePath;
    private HashSet<String> visitorsSet;
    private HashMap<String,Integer> hitsMap;
    private List<LogEntry> log;
    private int validEntriesNo, invalidEntriesNo;
    private static final String entryPattern =
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    private static final SimpleDateFormat serverFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US);
    private long runTime;

    public LogParser() {
        visitorsSet = new HashSet<>();
        hitsMap = new HashMap<>();
        log = new ArrayList<>();
        validEntriesNo = 0;
        invalidEntriesNo = 0;
        runTime = 0;
    }

    public void setLogFilePath(String path){
        logFilePath = path;
    }

    public int getInvalidEntriesNo() {
        return invalidEntriesNo;
    }

    public int getValidEntriesNo() {
        return validEntriesNo;
    }

    public List<LogEntry> getLog() {
        return log;
    }

    public void run(){
            try(BufferedReader reader = new BufferedReader(new FileReader(logFilePath))){

                // Parsing Data
                Pattern pattern = Pattern.compile(entryPattern);
                Matcher matcher;

                long startTime = System.currentTimeMillis();
                String currentLine;
                while ((currentLine = reader.readLine()) != null){
                    matcher = pattern.matcher(currentLine);
                    if (matcher.find()) {
                        LogEntry entry = new LogEntry();
                        entry.host = matcher.group(1);
                        entry.dateTime = serverFormat.parse(matcher.group(4).split(" ")[0]);
                        entry.zone = matcher.group(4).split(" ")[1];
                        entry.request = matcher.group(5);
                        int queryIndex = matcher.group(6).indexOf('?');
                        entry.path = queryIndex > 0 ? matcher.group(6).substring(0,queryIndex) : matcher.group(6);
                        entry.httpVersion = matcher.group(7);
                        entry.status =  Short.parseShort(matcher.group(8));
                        entry.sizeInBytes = Long.parseLong(matcher.group(9));

                        log.add(entry);
                        validEntriesNo++;
                    }
                    else
                        invalidEntriesNo++;
                }
                runTime = System.currentTimeMillis() - startTime;
            } catch (IOException | ParseException e) {
                e.printStackTrace();
            }
    }

    public void store(){
        try(FileWriter visitorsFile = new FileWriter("visitors.txt",false);
            FileWriter hitsFile = new FileWriter("hits.txt",false);
            FileWriter topHitsFile = new FileWriter("tophits.txt",false)){

            for (LogEntry entry : log){
                visitorsSet.add(entry.host);

                Integer hitsNo = hitsMap.get(entry.path);
                if (hitsNo == null)
                    hitsMap.put(entry.path,1);
                else
                    hitsMap.put(entry.path,hitsNo+1);
            }

            //Storing Data
            for (String ip : visitorsSet)
                visitorsFile.write(ip + "\n");

            for (Map.Entry<String, Integer> entry : hitsMap.entrySet()) {
                HashMap.Entry pair = (HashMap.Entry) entry;
                hitsFile.write(pair.getKey() + " , " + pair.getValue() + "\n");
            }

            //Sorting
            List<Map.Entry<String,Integer>> hitList = new ArrayList<>(hitsMap.entrySet());
            Collections.sort(hitList, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });

            for (Map.Entry<String,Integer> entry : hitList){
                topHitsFile.write(entry.getKey() + " , " + entry.getValue() + "\n");
            }

            visitorsFile.flush();
            hitsFile.flush();
            topHitsFile.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void printReport(){
        System.out.println("Http log parsing took " + runTime + "ms\n");
        System.out.println("Number of valid entries: " + validEntriesNo);
        System.out.println("Number of invalid entries: " + invalidEntriesNo);
        if (invalidEntriesNo > 1000)
            System.out.println("Well, it's not a perfect parser after all\n");
    }

        public static void main (String[] args){
            LogParser parser = new LogParser();

            if(args.length > 0)
                parser.setLogFilePath(args[0]);
            else
                parser.setLogFilePath("NASA_access_log_Jul95");

            parser.run();
            parser.store();
            parser.printReport();
        }
}
