var net = require('net');

var client = net.connect(4444, '192.168.99.100');
client.setEncoding('utf8');

setInterval(function() {
  console.log("sending...")
  var msg = Math.floor(Math.random()*10000);
  client.write('send mytopic 123 bajs'+msg+'\n');
}, 250)

client.on('data', function(data) {
  console.log('data was', data)
})
