import org.apache.log4j.Logger;

/**
 * Created by Zhaogw&Lss on 2019/11/27.
 */
public class LoggerGenerator {
    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws Exception{
        int index = 0;
        while (true){
            Thread.sleep(1000);
            logger.info("values:" + index++);
        }
    }

}