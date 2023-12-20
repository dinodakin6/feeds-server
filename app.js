const express = require('express');
const cors = require('cors');
const path = require('path');
const feedsRouter = require('./routers/generate-feed');

const compression = require('compression');

const app = express();

app.use(cors());
app.use(compression());
app.use(express.json());

app.use('/api/feeds', feedsRouter);

module.exports = app;
