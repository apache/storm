
var fs = require('fs');

function logToFile(msg) {

    fs.appendFileSync('/Users/anya/tmp/storm/log', msg + '\n\n\n');
}

function sendMsgToParent(msg){
    logToFile('SEND MESSAGE TO PARENT: ' + JSON.stringify(msg));
    var str = JSON.stringify(msg) + '\nend\n';
    process.stdout.write(str);
}

function sync(){
    sendMsgToParent({'command':'sync'});
}

function sendpid(heartbeatdir){
    var pid = process.pid;
    sendMsgToParent({'pid':pid})
    fs.closeSync(fs.openSync(heartbeatdir + "/" + pid, "w"));
}

function fail(tup) {
    sendMsgToParent({"command": "fail", "id": tup.id});
}

function log(msg) {
    sendMsgToParent({"command": "log", "msg": msg});
}

function Storm() {
    this.lines = [];
    this.taskIdCallbacks = [];
    this.numMessages = 0;
}

Storm.prototype.logToFile = function(msg) {
    logToFile(this.name + ':\n' + msg);
}

Storm.prototype.initSetupInfo = function(setupInfo) {
    sendpid(setupInfo['pidDir']);
    this.initialize(setupInfo['conf'], setupInfo['context']);
}

Storm.prototype.startReadingInput = function() {
    var self = this;
    this.logToFile('startReadingInput');

    process.stdin.on('readable', function() {
        var chunk = process.stdin.read();

        if (!!chunk && chunk.length !== 0) {
          var lines = chunk.toString().split('\n');
          lines.forEach(function(line) {
              self.handleNewLine(line);
          })
        }
    });
}

Storm.prototype.handleNewLine = function(line) {
    this.logToFile('handleNewLine LINE: ' + line);

    if (line === 'end') {
        this.logToFile('MESSAGE READY!!\n');
        var msg = this.collectMessageLines();
        this.cleanLines();
        this.handleNewMessage(msg);
    } else {
        this.storeLine(line);
    }
}

Storm.prototype.collectMessageLines = function() {
    return this.lines.join('\n');
}

Storm.prototype.cleanLines = function() {
    this.lines = [];
}

Storm.prototype.storeLine = function(line) {
    this.lines.push(line);
}

Storm.prototype.isFirstMsg = function() {
    return (this.numMessages === 0);
}

Storm.prototype.isTaskId = function(msg) {
    return (msg instanceof Array);
}

Storm.prototype.handleNewMessage = function(msg) {
    var parsedMsg = JSON.parse(msg);

    this.logToFile('handleNewMessage ' + msg);

    if (this.isFirstMsg()) {
        this.logToFile('first message');
        this.initSetupInfo(parsedMsg);
    } else if (this.isTaskId(parsedMsg)) {
        this.logToFile('task id');
        this.handleNewTaskId(parsedMsg);
    } else {
        this.logToFile('command');
        this.handleNewCommand(parsedMsg);
    }
    this.numMessages++;
}

Storm.prototype.handleNewTaskId = function(taskId) {
    var callback = this.taskIdCallbacks.shift();
    if (callback) {
        callback(taskId);
    }
}

Storm.prototype.emit = function(tup, stream, id, directTask, callback) {
    this.taskIdCallbacks.push(callback);
    this.__emit(tup, stream, id, directTask);
}

Storm.prototype.emitDirect = function(tup, stream, id, directTask) {
    this.__emit(tup, stream, id, directTask)
}

Storm.prototype.initialize = function(conf, context) {}

Storm.prototype.run = function() {
    this.logToFile('run');
    this.startReadingInput();
}

function Tuple(id, component, stream, task, values) {
    this.id = id;
    this.component = component;
    this.stream = stream;
    this.task = task;
    this.values = values;
}
//    def __repr__(self):
//        return '<%s%s>' % (
//                self.__class__.__name__,
//                ''.join(' %s=%r' % (k, self.__dict__[k]) for k in sorted(self.__dict__.keys())))

//function Bolt() {};
//
//Bolt.prototype.initialize = function(stormconf, context) {};
//
//Bolt.prototype.process = function(tuple) {};
//
//Bolt.prototype.run = function() {
//        MODE = Bolt
//        var setupInfo = initComponent();
//        var conf = setupInfo[0];
//        var context = setupInfo[1];
//
//        this.initialize(conf, context);
//        try {
//            while (true) {
//                var tup = readTuple();
//                this.process(tup);
//            }
//        } catch(err) {
//            log(err);
//        }
//}

function BasicBolt() {
    Storm.call(this);
    this.anchorTuple = null;
    this.name = 'BOLT'
};

BasicBolt.prototype = Object.create(Storm.prototype);
BasicBolt.prototype.constructor = Storm;

BasicBolt.prototype.process = function(tuple) {};

BasicBolt.prototype.__emit = function(tup, stream, anchors, directTask) {
    var self = this;
    if (typeof anchors === 'undefined') {
        anchors = [];
    }

    if (this.anchorTuple !== null) {
        this.logToFile('Anchor tuple id - ' + this.anchorTuple.id);
        anchors = [this.anchorTuple]
    }
    var m = {"command": "emit"};

    if (typeof stream !== 'undefined') {
        m["stream"] = stream
    }

    m["anchors"] = anchors.map(function (a) {
        self.logToFile('ID - ' + a.id);
        return a.id;
    });

    if (typeof directTask !== 'undefined') {
        m["task"] = directTask;
    }
    m["tuple"] = tup;
    sendMsgToParent(m);
}

BasicBolt.prototype.handleNewCommand = function(command) {
    var tup = new Tuple(command["id"], command["comp"], command["stream"], command["task"], command["tuple"]);
    this.logToFile('Anchor tuple: id - ' + command["id"] + ' tuple - ' + JSON.stringify(command['tuple']));
    this.anchorTuple = tup;
    this.process(tup);
    this.ack(tup);
}

BasicBolt.prototype.ack = function(tup) {
    sendMsgToParent({"command": "ack", "id": tup.id});
}

function Spout() {
    Storm.call(this);
    this.name = 'SPOUT';
};
Spout.prototype = Object.create(Storm.prototype);
Spout.prototype.constructor = Storm;

Spout.prototype.initialize = function(conf, context) {
    this.emit(['Spout Initializing']);
};

Spout.prototype.ack = function(id) {};

Spout.prototype.fail = function(id) {};

Spout.prototype.nextTuple = function(callback) {};

Spout.prototype.handleNewCommand = function(command) {
    var self = this;
    var callback = function() {
        sync();
    }

    if (command["command"] === "next") {
        this.nextTuple(callback);
    }

    if (command["command"] === "ack") {
        this.ack(command["id"], callback);
    }

    if (command["command"] === "fail") {
        this.fail(command["id"], callback);
    }
}

Spout.prototype.__emit = function(tup, stream, id, directTask) {
    var m = {"command": "emit"};
    if (typeof id !== 'undefined') {
        m["id"] = id;
    }

    if (typeof stream !== 'undefined') {
        m["stream"] = stream;
    }

    if (typeof directTask !== 'undefined') {
        m["task"] = directTask;
    }

    m["tuple"] = tup;
    sendMsgToParent(m);
}

module.exports.BasicBolt = BasicBolt;
module.exports.logToFile = logToFile;
module.exports.Spout = Spout;
module.exports.log = log;
