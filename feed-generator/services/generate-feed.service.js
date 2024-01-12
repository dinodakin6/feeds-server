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
  const { merchantName, merchantId, requestId } = e;

  await updateRegenerateData(merchantId, 'done', requestId);
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
    const { merchantName, merchantId, requestId } = nextRequest;
    await regenerateFeed(merchantName, merchantId, requestId);
  }
};

const enqueueRequest = (merchantName, merchantId) => {
  let requestId = Math.random().toString(36).substring(3, 9);
  regenerationQueue.push({ merchantName, merchantId, requestId });
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

const uploadFeed = async (merchantName, merchantId, requestId) => {
  flog('Start of uploadFeed');
  await updateRegenerateData(merchantId, 'uploading', requestId);

  const files = fs.readdirSync(FEED.META.FEED_PATH);
  const textFile = files.filter(item => item.includes(merchantName));

  flog(`Uploading ${textFile}`);
  const filePath = `${FEED.META.FEED_PATH}/${textFile}`;
  await uploadBingFeedToS3({ filePath, key: textFile.toString() });

  flog('-- done uploadFeed');
};

const regenerateFeed = async (merchantName, merchantId, requestId) => {
  flog('regenerateFeed');
  const start = new Date().toISOString();
  const startLocale = new Date(start).toLocaleString();

  try {
    isRegenerating = true;
    const toFetch = process.argv[2] !== '--no-refetch';
    flog(`-- to fetch: ${toFetch}`);
    await insertRegenerateData(merchantId, merchantName, requestId);

    if (toFetch) await getSpecificMerchantFeed(merchantId);
    await updateRegenerateData(merchantId, 'regenerating', requestId);
    await uploadFeed(merchantName, merchantId, requestId);

    const end = new Date().toISOString();
    const endLocale = new Date(end).toLocaleString();

    emitter.emit('sendWebhook', {
      merchantName,
      merchantId,
      requestId,
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
    const res = await axios.post(process.env.WEBHOOK_URL, data);
    flog(`--Finished sending hook to VLM: ${res.message}`);
  } catch (error) {
    console.log(error);
    flog('--Fail sending upload hook to VLM--');
  }
};

module.exports = {
  enqueueRequest,
};
