package com.alibaba.jstorm.yarn;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

	private static final Logger LOG = LoggerFactory.getLogger(Client.class);

    public static interface ClientCommand {

        /**
         * @return the options this client will process.
         */
        public Options getOpts();

        /**
         * @return header description for this command
         */
        public String getHeaderDescription();
        
        /**
         * Do the processing
         * @param cl the arguments to process
         * @param stormConf the storm configuration to use
         * @throws Exception on any error
         */
        public void process(CommandLine cl) throws Exception;
    }

    public static class HelpCommand implements ClientCommand {
        HashMap<String, ClientCommand> _commands;
        public HelpCommand(HashMap<String, ClientCommand> commands) {
            _commands = commands;
        }

        @Override
        public Options getOpts() {
            return new Options();
        }

        @Override
        public String getHeaderDescription() {
          return "jstorm-yarn help";
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void process(CommandLine cl) throws Exception {
            printHelpFor(cl.getArgList());
        }

        public void printHelpFor(Collection<String> args) {
            if(args == null || args.size() < 1) {
                args = _commands.keySet();
            }
            HelpFormatter f = new HelpFormatter();
            for(String command: args) {
                ClientCommand c = _commands.get(command);
                if (c != null) {
                    //TODO Show any arguments to the commands.
                    f.printHelp(command,  c.getHeaderDescription(), c.getOpts(), null);
                } else {
                    System.err.println("ERROR: " + c + " is not a supported command.");
                    //TODO make this exit with an error at some point
                }
            }
        }
    }
    
    /**
     * @param args the command line arguments
     * @throws Exception  
     */    
    @SuppressWarnings("rawtypes")
    public void execute(String[] args) throws Exception {
        HashMap<String, ClientCommand> commands = new HashMap<String, ClientCommand>();
        HelpCommand help = new HelpCommand(commands);
        commands.put("help", help);
        commands.put("launch", new LaunchCommand());
        commands.put("setStormConfig", new StormMasterCommand(StormMasterCommand.COMMAND.SET_STORM_CONFIG));
        commands.put("getStormConfig", new StormMasterCommand(StormMasterCommand.COMMAND.GET_STORM_CONFIG));
        commands.put("addSupervisors", new StormMasterCommand(StormMasterCommand.COMMAND.ADD_SUPERVISORS));
        commands.put("startNimbus", new StormMasterCommand(StormMasterCommand.COMMAND.START_NIMBUS));
        commands.put("stopNimbus", new StormMasterCommand(StormMasterCommand.COMMAND.STOP_NIMBUS));
        commands.put("startUI", new StormMasterCommand(StormMasterCommand.COMMAND.START_UI));
        commands.put("stopUI", new StormMasterCommand(StormMasterCommand.COMMAND.STOP_UI));
        commands.put("startSupervisors", new StormMasterCommand(StormMasterCommand.COMMAND.START_SUPERVISORS));
        commands.put("stopSupervisors", new StormMasterCommand(StormMasterCommand.COMMAND.STOP_SUPERVISORS));
        commands.put("shutdown", new StormMasterCommand(StormMasterCommand.COMMAND.SHUTDOWN));
        commands.put("version", new VersionCommand());
        
        String commandName = null;
        String[] commandArgs = null;
        if (args.length < 1) {
            commandName = "help";
            commandArgs = new String[0];
        } else {
            commandName = args[0];
            commandArgs = Arrays.copyOfRange(args, 1, args.length);
        }
        ClientCommand command = commands.get(commandName);
        if(command == null) {
            LOG.error("ERROR: " + commandName + " is not a supported command.");
            help.printHelpFor(null);
            System.exit(1);
        }
        Options opts = command.getOpts();
        if(!opts.hasOption("h")) {
            opts.addOption("h", "help", false, "print out a help message");
        }
        CommandLine cl = new GnuParser().parse(command.getOpts(), commandArgs);
        if(cl.hasOption("help")) {
            help.printHelpFor(Arrays.asList(commandName));
        } else {
           
            command.process(cl);
        }
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client();
        client.execute(args);
    } 

}
