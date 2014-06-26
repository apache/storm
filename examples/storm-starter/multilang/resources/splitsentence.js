var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function SplitSentenceBolt() {
    BasicBolt.call(this);
};

SplitSentenceBolt.prototype = new BasicBolt();
SplitSentenceBolt.prototype = Object.create(BasicBolt.prototype);

SplitSentenceBolt.prototype.process = function(tup) {
        var self = this;
        var words = tup.values[0].split(" ");
        words.forEach(function(word) {
            self.emit([word], null, null, null, function(taskId) {
                storm.logToFile('Task id - ' + JSON.stringify(taskId) + ' work - ' + word);
            });
        });
}

SplitSentenceBolt.prototype.initialize = function(conf, context) {
    storm.logToFile("CONF: " + JSON.stringify(conf));
    storm.logToFile("CONTEXT: " + JSON.stringify(context));
}

new SplitSentenceBolt().run();