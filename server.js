const http = require('http');
const app = require('./app');
const server = http.createServer(app);

server.listen(5001, () => {
  console.log('listening to 5001 ', new Date().toLocaleString());
});
