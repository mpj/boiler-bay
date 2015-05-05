var net = require('net');

var client = net.connect(4444, 'localhost');
client.setEncoding('utf8');
setTimeout(function() {
  client.write('consume mytopic mygroupsz smallest\n');
}, 500);

setTimeout(function() {
  client.write('next\n');
}, 1000)

client.on('data', function(data) {
  console.log('data was', data)
  if (data === 'commit-ok\n')
    client.write('next\n');
  if (data.indexOf('msg') > -1) {
    console.log("got msg", data)
    client.write('commit\n');
  }
})