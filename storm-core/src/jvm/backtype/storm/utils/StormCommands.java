package backtype.storm.utils;

/**
 * Created by pshah on 7/17/15.
 */
public class StormCommands {
    public static void main (String[] args) {
        System.out.println("Executing StormCommands main method");
        for (String arg: args) {
            System.out.println("Argument ++ is " + arg);

        }
    }
}
