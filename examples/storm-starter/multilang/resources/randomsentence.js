/**
 * Created by anya on 6/26/14.
 */

var storm = require('./storm');
var Spout = storm.Spout;


var SENTENCES = [
    "the cow jumped over the moon",
    "an apple a day keeps the doctor away",
    "four score and seven years ago",
    "snow white and the seven dwarfs",
    "i am at two with nature"]

function RandomSentenceSpout() {
    Spout.call(this);
    this.runningTupleId = getRandomInt(0,Math.pow(2,16));
    this.pending = {};

    storm.logToFile('CREATE NEW RandomSentenceSpout');
};

RandomSentenceSpout.prototype = new Spout();
RandomSentenceSpout.prototype = Object.create(Spout.prototype);

RandomSentenceSpout.prototype.getRandomSentence = function() {
    return SENTENCES[getRandomInt(0, SENTENCES.length - 1)];
}

RandomSentenceSpout.prototype.initialize = function(conf, context) {
    storm.logToFile("CONF: " + JSON.stringify(conf));
    storm.logToFile("CONTEXT: " + JSON.stringify(context));
}

RandomSentenceSpout.prototype.nextTuple = function(callback) {
    var sentence = this.getRandomSentence();
    var tup = [sentence];
    var id = this.runningTupleId;
    this.pending[id] = tup;
    this.emit(tup, null, id, null);
    this.runningTupleId++;
    callback();
}

RandomSentenceSpout.prototype.ack = function(id, callback) {
    this.logToFile('RECEIVED ACK - ' + JSON.stringify(id));
    delete this.pending[id];
    callback();
}

RandomSentenceSpout.prototype.fail = function(id, callback) {
    this.logToFile('RECEIVED FAIL - ' + JSON.stringify(id));
    this.emit(this.pending[id], null, id, null);
    callback();
}

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

new RandomSentenceSpout().run();
