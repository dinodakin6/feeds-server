const { promisify } = require('util');
const axios = require('axios');
const zlib = require('zlib');
const fs = require('fs');

const { readCsvToObjectArray } = require('../lib');
const FEED = require('./constants');

const unzipAndSaveFile = async (fileStream, fileName) => {
  const promiseUnzip = promisify(zlib.gunzip);
  const promiseWrite = promisify(fs.writeFile);

  const file = await promiseUnzip(fileStream);
  const feedDirectory = FEED.META.ROOT_PATH;

  await promiseWrite(`${feedDirectory}/${fileName}`, file);

  return file;
};

const readPromise = ({ readPath, onData, onEnd }) => {
  const readStream = fs.createReadStream(readPath);

  return new Promise((resolve, reject) => {
    readStream.on('data', onData);
    readStream.on('end', () => {
      onEnd();
      resolve();
    });
    readStream.on('error', () => {
      onEnd();
      reject();
    });
  });
};

/**
 * Auto-create the directory to store your feed.
 * Directory path is based on env BING_ROOT_PATH
 *
 * @param {boolean} purgeDirectory - If true, deletes the directory and creates it again. If false, just creates the directory
 *
 */
const prepareFeedDirectories = ({ purgeDirectory }) => {
  if (purgeDirectory) {
    fs.rmSync(FEED.META.ROOT_PATH, { recursive: true, force: true });
  }

  fs.mkdirSync(FEED.META.FEED_PATH, { recursive: true });
};

/**
 * Downloads and saves offer JSON index and PLA index as "index.txt" and "index_pla.txt". Saves in BING_ROOT_PATH
 *
 */
const downloadIndexFiles = async () => {
  const toDownload = [FEED.DEFAULT, FEED.PLA];
  const params = FEED.META.PARAMS;

  for (const item of toDownload) {
    const url = `${item.URL}/${params}`;
    const response = await axios.get(url, { responseType: 'arraybuffer' });
    await unzipAndSaveFile(response.data, item.NAME);
  }
};

/**
 * Downloads and saves merchant JSON file as "merchants.json". Saves in BING_ROOT_PATH
 *
 */
const downloadMerchantFile = async () => {
  const merchantUrl = FEED.MERCHANT.URL;
  const params = FEED.META.PARAMS;
  const fileName = FEED.MERCHANT.NAME;

  const url = `${merchantUrl}/${params}`;
  const response = await axios.get(url, { responseType: 'arraybuffer' });
  await unzipAndSaveFile(response.data, fileName);
};

/**
 * Get all merchant details based on merchant file. If file does not exist, this will throw an error
 *
 */
const getMerchants = () => {
  const path = `${FEED.META.ROOT_PATH}/${FEED.MERCHANT.NAME}`;

  const merchantsJson = JSON.parse(fs.readFileSync(path, 'utf-8'));

  const merchantsMap = merchantsJson.merchant.reduce((acc, item) => {
    const newMerchantInfo = {
      ...item.merchantInfo,
      id: item.mid,
    };

    acc[item.mid] = newMerchantInfo;
    return acc;
  }, {});

  return merchantsMap;
};

/**
 * Downloads and saves ecpc multiplier file as "ecpc_multiplier_feed.csv". Saves in BING_ROOT_PATH
 *
 */
const downloadEcpcMultiplier = async () => {
  const multiplierUrl = FEED.MULTIPLIER.URL;
  const fileName = FEED.MULTIPLIER.NAME;
  const params = FEED.META.PARAMS;

  const url = `${multiplierUrl}${params}`;
  const response = await axios.get(url, { responseType: 'arraybuffer' });
  await unzipAndSaveFile(response.data, fileName);
};

/**
 * Get an array of objects of multiplier details based on "ecpc_multiplier_feed.csv". If file does not exist, this will throw an error
 *
 */
const getMultiplier = () => {
  const multiplierData = readCsvToObjectArray(`${FEED.META.ROOT_PATH}/${FEED.MULTIPLIER.NAME}`);
  return multiplierData;
};

/**
 * Downloads an array of offer files and saves it under BING_ROOT_PATH/feeds
 *
 * @param {OfferLink[]} links - An array of objects describing the offer links to download
 * @param {boolean} purgeDirectory - If true, delete all existing files in BING_ROOT_PATH/feeds before downloading
 * @param {FeedType} type - use DEFAULT if downloading offer JSON files. use PLA if downloading offer PLA files (csv)
 *
 */
const downloadOfferFiles = async ({ links, purgeDirectory, type }) => {
  if (purgeDirectory) {
    fs.rmSync(FEED.META.FEED_PATH, { recursive: true, force: true });
    fs.mkdirSync(FEED.META.FEED_PATH);
  }

  const fileType = FEED[type].TYPE;
  const params = FEED.META.PARAMS;

  for (const link of links) {
    const withParams = `${link.url}/${params}`;
    const fileName = `${link.name}.${fileType}`;

    const response = await axios.get(withParams, {
      responseType: 'arraybuffer',
      headers: {
        'Accept-Encoding': 'gzip, deflate',
      },
    });
    console.log('-- done downloading ', link.name);
    await unzipAndSaveFile(response.data, `feeds/${fileName}`);
  }
};

/**
 * Returns an array of objects containing the details of all downloaded offer files saved under BING_ROOT_PATH/feeds
 *
 * @param {string} indexType - DEFAULT if you want to get the offer JSON files. PLA if you want to get offer PLA files (csv)
 *
 */
const getOfferLinkFromIndex = ({ indexType, merchantId }) => {
  const indexFileName = FEED[indexType].NAME;
  const indexRegex =
    FEED[indexType].NAME === 'index_pla.txt'
      ? `\/(${merchantId}_pla_part\\d{3}).(?:json|csv).gz$`
      : `\/(${merchantId}_part\\d{3}).(?:json|csv).gz$`;
  const directory = FEED.META.ROOT_PATH;

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(`${directory}/${indexFileName}`);

    let contents = '';

    stream.on('data', line => {
      contents += line.toString();
    });

    stream.on('end', () => {
      const contentArray = contents.split('\n');

      const areOfferParts = contentArray.reduce((acc, line) => {
        const offerPart = line.trim().match(indexRegex);

        if (!offerPart) return acc;

        acc.push({
          name: offerPart[1],
          url: line,
          merchantId: merchantId,
          type: indexType,
        });

        return acc;
      }, []);

      stream.close();
      resolve(areOfferParts);
    });

    stream.on('error', e => {
      reject(e);
    });
  });
};

/**
 * Combines all provided PLA CSV files into one large CSV file. Saves it under BING_ROOT_PATH/feeds. Name will be combined_merchantid123.csv
 *
 * @param {string} plaLinks - array of objects describing the PLA files to include in the combining
 *
 */
const generateCombinedPlaFeeds = async ({ plaLinks }) => {
  for (const links of plaLinks) {
    const sourcePath = `${FEED.META.FEED_PATH}/${links.name}.csv`;
    const destination = `${FEED.META.FEED_PATH}/combined_${links.merchantId}.csv`;
    console.log(`-- combining ${links.name} to ${links.merchantId}`);

    const writeStream = fs.createWriteStream(destination, { flags: 'a' });

    const onEnd = () => {
      writeStream.close();
    };

    const combinedFeedExists = fs.existsSync(destination);

    if (!combinedFeedExists) {
      fs.copyFileSync(sourcePath, destination);
      writeStream.close();
      continue;
    }

    let skipHeader = true;

    const onData = chunk => {
      if (!skipHeader) {
        writeStream.write(chunk);
      } else {
        const firstNewlineIndex = chunk.indexOf('\n');
        if (firstNewlineIndex !== -1) {
          writeStream.write(chunk.slice(firstNewlineIndex + 1));
        }
        skipHeader = false;
      }
    };

    await readPromise({ readPath: sourcePath, onData, onEnd });
  }
  console.log('-- done combining. length: ', plaLinks.length);
};

module.exports = {
  downloadEcpcMultiplier,
  downloadIndexFiles,
  downloadMerchantFile,
  downloadOfferFiles,
  generateCombinedPlaFeeds,
  getMerchants,
  getMultiplier,
  getOfferLinkFromIndex,
  prepareFeedDirectories,
};
