var net = require('net');

var client = net.connect(4444, 'localhost');
client.setEncoding('utf8');

setInterval(function() {
  console.log("sending...")
  var msg = Math.floor(Math.random()*10000);
  client.write('send mytopic 123 bajs'+msg+'\n');
}, 2000)

client.on('data', function(data) {
  console.log('data was', data)
})
