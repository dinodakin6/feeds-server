const { flog, saveTextToFile } = require('../lib');
const {
  downloadEcpcMultiplier,
  downloadIndexFiles,
  downloadMerchantFile,
  downloadOfferFiles,
  generateCombinedPlaFeeds,
  getMerchants,
  getMultiplier,
  getOfferLinkFromIndex,
  prepareFeedDirectories,
} = require('../connexity/service');

const { insertRegenerateData, updateRegenerateData } = require('./update-db.service');

const axios = require('axios');
const fs = require('fs');

const BingService = require('../bing/service');
const { uploadBingFeedToS3 } = require('./feed-upload.service');
const FEED = require('../connexity/constants');
const event = require('events');

const emitter = new event.EventEmitter();

emitter.on('sendWebhook', async e => {
  const { merchantName, merchantId } = e;

  await updateRegenerateData(merchantId, 'done');
  await sendUploadHook(merchantName, merchantId);
});

const regenerationQueue = [];
let isRegenerating = false;

const processQueue = async () => {
  if (isRegenerating || regenerationQueue.length === 0) {
    return;
  }

  const nextRequest = regenerationQueue.shift();

  if (nextRequest) {
    const { merchantName, merchantId } = nextRequest;
    await regenerateFeed(merchantName, merchantId);
  }
};

const enqueueRequest = (merchantName, merchantId) => {
  regenerationQueue.push({ merchantName, merchantId });
  processQueue();
};

const getSpecificMerchantFeed = async mid => {
  flog(`Start of getSpecificMerchantFeed: ${mid}`);
  prepareFeedDirectories({ purgeDirectory: true });

  flog('-- downloading all index');
  await downloadIndexFiles();
  flog('-- downloading all merchants');
  await downloadMerchantFile();
  flog('-- downloading multiplier');
  await downloadEcpcMultiplier();

  const offerLinks = await getOfferLinkFromIndex({ indexType: 'DEFAULT', merchantId: mid });
  const plaLinks = await getOfferLinkFromIndex({ indexType: 'PLA', merchantId: mid });

  console.log(offerLinks, plaLinks);

  flog('-- downloading default offers');
  await downloadOfferFiles({ links: offerLinks, type: 'DEFAULT', purgeDirectory: false });
  flog('-- downloading pla offers');
  await downloadOfferFiles({ links: plaLinks, type: 'PLA', purgeDirectory: false });

  flog('-- generating combined pla feeds');
  await generateCombinedPlaFeeds({ plaLinks: plaLinks });

  const merchantsMap = getMerchants();
  const multiplierContents = getMultiplier();

  const rejectedMerchants = [];

  const merchantId = plaLinks[0].merchantId;
  flog(`-- creating feed file for ${merchantId}`);

  const merchantName = merchantsMap[merchantId].name;

  const sourcePath = `${FEED.META.FEED_PATH}/combined_${merchantId}.csv`;
  const destination = `${FEED.META.FEED_PATH}/${merchantName}.txt`;

  const merchantInfo = merchantsMap[merchantId];

  const feedFile = await BingService.getAdditionalFeedData(merchantId);

  if (merchantInfo) {
    await BingService.createFeedFile({
      merchantInfo,
      sourcePath,
      destination,
      feedFile: feedFile,
      multiplierContents,
    });
  } else {
    rejectedMerchants.push(link.merchantId);
  }

  saveTextToFile({
    data: rejectedMerchants.join('\n'),
    filePath: '../logs/rejected_merchants.txt',
  });
  flog('-- done getAllFeeds');
};

const uploadFeed = async (merchantName, merchantId) => {
  flog('Start of uploadFeed');
  await updateRegenerateData(merchantId, 'uploading');

  const files = fs.readdirSync(FEED.META.FEED_PATH);
  const textFile = files.filter(item => item.includes(merchantName));

  flog(`Uploading ${textFile}`);
  const filePath = `${FEED.META.FEED_PATH}/${textFile}`;
  await uploadBingFeedToS3({ filePath, key: textFile.toString() });

  flog('-- done uploadFeed');
};

const regenerateFeed = async (merchantName, merchantId) => {
  flog('regenerateFeed');
  const start = new Date().toISOString();
  const startLocale = new Date(start).toLocaleString();

  try {
    isRegenerating = true;
    const toFetch = process.argv[2] !== '--no-refetch';
    flog(`-- to fetch: ${toFetch}`);
    await insertRegenerateData(merchantId, merchantName);

    if (toFetch) await getSpecificMerchantFeed(merchantId);
    await updateRegenerateData(merchantId, 'regenerating');
    await uploadFeed(merchantName, merchantId);

    const end = new Date().toISOString();
    const endLocale = new Date(end).toLocaleString();

    emitter.emit('sendWebhook', {
      merchantName,
      merchantId,
    });

    flog(`Start: --- ${start} --- ${startLocale}`);
    flog(`End: --- ${end} --- ${endLocale}`);
  } catch (error) {
    flog(error);
  } finally {
    isRegenerating = false;
    processQueue();
  }
};

const sendUploadHook = async (merchantName, merchantId) => {
  try {
    const data = {
      success: true,
      merchantName: merchantName,
      merchantId: merchantId,
    };
    const res = await axios.post(`http://localhost:5000/api/feeds/f`, data);
    flog(`--Finished sending hook to VLM: ${res.message}`);
  } catch (error) {
    console.log(error);
    flog('--Fail sending upload hook to VLM--');
  }
};

module.exports = {
  enqueueRequest,
};
