const dotenv = require('dotenv');
dotenv.config();

const pubId = process.env.CNX_PUBLISHER_ID;
const apiKey = process.env.CNX_API_KEY;
const bingRootPath = process.env.BING_ROOT_PATH;

const FEED = {
  PLA: {
    NAME: 'index_pla.txt',
    TYPE: 'csv',
    REGEX: /\/((\d+)_pla_part\d{3}).(?:json|csv).gz$/,
    URL: 'https://publisherexports-beta.connexity.com/feeds/mid/index_pla.txt.gz',
  },
  DEFAULT: {
    NAME: 'index.txt',
    TYPE: 'json',
    REGEX: /\/((\d+)_part\d{3}).(?:json|csv).gz$/,
    URL: 'http://publisherexports.connexity.com/feeds/mid/index.txt.gz',
  },
  MERCHANT: {
    NAME: 'merchants.json',
    TYPE: 'json',
    REGEX: null,
    URL: 'https://publisherexports.connexity.com/feeds/mid/merchants.json.gz',
  },
  MULTIPLIER: {
    NAME: 'ecpc_multiplier_feed.csv',
    TYPE: 'csv',
    REGEX: null,
    URL: 'http://publisherexports.connexity.com/feeds/ecpc_multiplier_feed.csv.gz',
  },
  META: {
    PARAMS: `?publisherId=${pubId}&apiKey=${apiKey}`,
    ROOT_PATH: bingRootPath,
    FEED_PATH: `${bingRootPath}/feeds`,
  },
};

module.exports = FEED;
