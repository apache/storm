package backtype.storm.utils;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.cli.*;

/**
 * Created by pshah on 7/17/15.
 */
public class StormCommands {
    public static void main (String[] args) {
        System.out.println("Executing StormCommands main method");
        for (String arg: args) {
            System.out.println("Argument ++ is " + arg);

        }
        Options options = new Options();
        options.addOption("jarcommand", true, "help on jar command");
        CommandLineParser cmdParser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = cmdParser.parse(options, args);
        } catch (ParseException pe) {
            System.out.println("Parse Exception" + pe.getMessage());
        }
        System.out.println("Value for jar command is " + cmd.getOptionValue
                ("jarcommand"));
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("storm", options);
    }
}
