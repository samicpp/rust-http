const { Socket } = require('dgram');
const net = require('net');
const readline = require('readline');
const client = new net.Socket();

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: true
});

client.connect(8080, '127.0.0.1', function() {
	console.log('opened');
});

client.on('data', function(data) {
	console.log("\x1b[31m",data.toString(),"\x1b[0m");
});

client.on('close', function() {
	console.log('closed');
    process.exit();
});


rl.on('line', (line) => {
    console.log("\x1b[32m",line,"\x1b[0m");
    client.write(line);
});