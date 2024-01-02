const mongoose = require('mongoose');
const dotenv = require('dotenv');
const path = require('path');

dotenv.config();

const env = process.env.NODE_ENV?.trim();

let options = {};

const dbName = process.env.DATABASE_NAME;
const dbUser = process.env.DATABASE_USER;
const dbPass = process.env.DATABASE_PASS;
const dbUrl = process.env.DATABASE_URL;
const caPath = 'cert/global-bundle.pem';

let connectionUrl = 'mongodb://localhost:27017/voluum';

if (env !== 'dev') {
  options = {
    dbName,
    tls: true,
    tlsCAFile: path.resolve(caPath),
    replicaSet: 'rs0',
    readPreference: 'secondaryPreferred',
    retryWrites: 'false',
  };

  connectionUrl = `mongodb://${dbUser}:${dbPass}@${dbUrl}`;
}

console.log('DB URL :>> ', connectionUrl);
console.log('DB OPTIONS :>> ', options);

const connection = mongoose.createConnection(connectionUrl, options);

connection.addListener('connecting', () => console.log('connecting to mongodb ', env));
connection.addListener('connected', () => console.log('connected to mongodb ', env));
connection.addListener('disconnected', e => console.log('db disconnected ', e));
connection.addListener('error', e => console.log('db error ', e));

module.exports = connection;
