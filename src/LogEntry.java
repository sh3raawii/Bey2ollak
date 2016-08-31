import java.util.Date;

/**
 * Created by Mostafa on 8/31/16.
 */

public class LogEntry{
    Date dateTime;
    String host, request, path, httpVersion, zone;
    short status;
    long sizeInBytes;
}