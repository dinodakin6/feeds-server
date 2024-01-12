const fs = require('fs');
const _ = require('lodash');
const JSONStream = require('JSONStream');
const { integralToUSDString, removeQuotes, rpcRange } = require('../lib');
const csv = require('csv-parser');
const FEED = require('../connexity/constants');

const labelMap = {
  Female_Desktop: [
    '947510000',
    '948510000',
    '949510000',
    '950510000',
    '951510000',
    '952510000',
    '953510000',
    '954510000',
    '956510000',
    '957510000',
    '958510000',
    '959510000',
    '960510000',
    '962510000',
    '963510000',
    '964510000',
    '965510000',
    '967510000',
    '968510000',
    '969510000',
  ],

  Female_Mobile: [
    '947410000',
    '948410000',
    '949410000',
    '950410000',
    '951410000',
    '952410000',
    '953410000',
    '954410000',
    '956410000',
    '957410000',
    '958410000',
    '959410000',
    '960410000',
    '962410000',
    '963410000',
    '964410000',
    '965410000',
    '967410000',
    '968410000',
    '969410000',
  ],
  Male_Desktop: [
    '947530000',
    '948530000',
    '949530000',
    '952530000',
    '953530000',
    '954530000',
    '956530000',
    '957530000',
    '958530000',
    '967530000',
    '968530000',
  ],
  Male_Mobile: [
    '947430000',
    '948430000',
    '949430000',
    '952430000',
    '953430000',
    '954430000',
    '956430000',
    '957430000',
    '958430000',
    '967430000',
    '968430000',
  ],
  Mix_Desktop: ['961520000'],
  Mix_Mobile: ['961420000'],
};

const placementIdsMap = {
  24: ['913510000', '913410000', '913530000', '913430000'],
  253317: ['941510000', '941410000', '941530000', '941430000'],
  25514: ['918530000', '918430000'],
  325965: ['934510000', '934410000'],
  190411: ['977510051', '977411102', '977530000', '977430000'],
  29430: ['926510000', '926410000', '926530000', '926430000'],
  321021: ['935510000', '935410000', '935530000', '935430000'],
  319903: ['932530000', '932430000'],
  19246: ['917510000', '917410000', '917530000', '917430000'],
  158466: ['924530000', '924430000'],
  28614: ['939510000', '939410000'],
  265795: ['919520000', '919420000'],
  32251: ['903510000', '903410000'],
  199042: ['929510020', '929410000', '929530012', '929430000'],
  315724: ['902510000', '902410000', '902530000', '902430000'],
  281380: ['944510000', '944410000'],
  301179: ['936510000', '936410000'],
  321920: ['937510000', '937410000', '937530000', '937430000'],
  24442: ['910510000', '910410000'],
  155524: ['940510000', '940410000'],
  61127: ['906510000', '906410000'],
  242134: ['911510000', '911410000'],
  44084: ['920520000', '920420000'],
  309295: ['921510000', '921410000', '921530000', '921430000'],
  28592: ['908530012', '908430000'],
  317246: ['901510000', '901410000', '901530000', '901430000'],
  25533: ['942510000', '942410000', '942530000', '942430000'],
  303087: ['999510000', '999410000', '999530000', '999430000'],
  313780: ['928510000', '928410000', '928530000', '928430000'],
  164388: ['915510000', '915410000', '915530000', '915430000'],
  77064: ['912510000', '912410000', '912530000', '912430000'],
  315846: ['931510000', '931410000', '931530000', '931430000'],
  149: ['923520000', '923420000'],
  252092: ['904510000', '904410000'],
  32913: ['900530000', '900430000'],
  288451: ['916520000', '916420000'],
  166203: ['927530000', '927430000'],
  299459: ['938510000', '938410000', '938530000', '938430000'],
  315385: ['943510000', '943410000', '943530000', '943430000'],
  315684: ['930510000', '930410000', '930530000', '930430000'],
  30783: ['933510000', '933410000'],
  27754: ['925510000', '925410000', '925530000', '925430000'],
  67528: ['955510051', '955410015'],
  320952: ['909520102', '909420102'],
  317217: ['945510000', '945410000', '945530000', '945430000'],
  309659: ['999510051', '999410000', '988530051', '988430051'],
  17330: ['907510000', '907410000'],
  37130: ['946510000', '946410000', '946530000', '946430000'],
  30782: ['922511100', '922410000'],
  197931: ['966510000', '966410000', '966530000', '966430000'],
  257468: ['905530405', '905430102'],
  662: ['968510000', '968410000', '968530000', '968430000'],
  324280: ['969510000', '969410000'],
  303590: ['947510000', '947410000', '947530000', '947430000'],
  19881: ['948510000', '948410000', '948530000', '948430000'],
  317089: ['949510000', '949410000', '949530000', '949430000'],
  275408: ['950510000', '950410000'],
  102927: ['951510000', '951410000'],
  220816: ['952510000', '952410000', '952530000', '952430000'],
  326387: ['953510000', '953410000', '953530000', '953430000'],
  299275: ['954510000', '954410000', '954530000', '954430000'],
  200942: ['956510000', '956410000', '956530000', '956430000'],
  263406: ['957510000', '957410000', '957530000', '957430000'],
  172722: ['958510000', '958410000', '958530000', '958430000'],
  29666: ['959510000', '959410000'],
  317263: ['960510000', '960410000'],
  202334: ['961520000', '961420000'],
  170918: ['962510000', '962410000'],
  216562: ['963510000', '963410000'],
  277057: ['964510000', '964410000'],
  317481: ['965510000', '965410000'],
  207794: ['967510000', '967410000', '967530000', '967430000'],
};

const replacements = {
  28592: [
    {
      check: '908530012',
      replaceWith: '908530000',
    },
  ],
  30782: [
    {
      check: '922511100',
      replaceWith: '922510000',
    },
  ],
  190411: [
    {
      check: '977510051',
      replaceWith: '977510102',
    },
    {
      check: '977411102',
      replaceWith: '977410000',
    },
  ],
  199042: [
    {
      check: '929510020',
      replaceWith: '929510000',
    },
    {
      check: '929530012',
      replaceWith: '929530000',
    },
  ],
  67528: [
    {
      check: '955510051',
      replaceWith: '955510015',
    },
    {
      check: '955410015',
      replaceWith: '955410051',
    },
  ],
};

const defaultMultipliers = {
  190411: 0.2,
  257468: 0.25,
};

const BingAttributes = {
  ID: 'id',
  TITLE: 'title',
  DESCRIPTION: 'description',
  PRODUCT_LINK: 'link',
  IMAGE_LINK: 'image_link',
  PRICE: 'price',
  SALE_PRICE: 'sale_price',
  PRODUCT_CATEGORY: 'product_category',
  GENDER: 'gender',
  AGE_GROUP: 'age_group',
  COLOR: 'color',
  SIZE: 'size',
  BRAND: 'brand',
  GTIN: 'gtin',
  MPN: 'mpn',
  SHIPPING_COST: 'shipping',
  IDENTIFIER_EXISTS: 'identifier_exists',
  SELLER_NAME: 'seller_name',
  ADS_REDIRECT_LINK: 'ads_redirect',
  LABEL_0: 'custom_label_0',
  LABEL_1: 'custom_label_1',
  LABEL_2: 'custom_label_2',
  LABEL_3: 'custom_label_3',
};

const getGenderValue = gender => {
  switch (gender) {
    case 'boys':
    case 'girls':
      return 'kids';
    case 'male':
    case 'female':
    case 'unisex':
      return gender;
    default:
      return 'unknown';
  }
};

const extractBingAttributes = (merchantInfo, offerJson, additionalFeedData, placementId) => {
  const neededAttributes = {
    id: 'id',
    title: offer => {
      if (!offer.title) return '';
      return removeQuotes(offer.title);
    },
    description: offer => {
      if (!offer.description) return '';
      return removeQuotes(offer.description);
    },
    link: () => {
      const merchantId = merchantInfo.id;
      const { merchantProductId } = additionalFeedData;

      return `https://onlinebazaar4u.com/product/${merchantId}/${merchantProductId}`;
    },
    image_link: 'image_link',
    price: 'price',
    sale_price: 'sale_price',
    product_category: 'google_product_category',
    gender: 'gender',
    age_group: 'age_group',
    color: 'color',
    size: 'size',
    brand: 'brand',
    gtin: 'gtin',
    mpn: 'mpn',
    shipping: () => {
      return integralToUSDString(additionalFeedData.shipping);
    },
    identifier_exists: offer => {
      const hasBrandAndGtin = offer.brand && offer.gtin;
      const hasBrandAndMpn = offer.brand && offer.mpn;

      if (!hasBrandAndGtin && !hasBrandAndMpn) return 'FALSE';
      return 'TRUE';
    },
    seller_name: () => {
      const name = merchantInfo.name;
      return name;
    },
    ads_redirect: () => {
      const res = additionalFeedData.url.replace(/&af_placement_id=1/g, '');
      return res;
    },
    custom_label_0: () => {
      const name = merchantInfo.name;
      return name;
    },
    custom_label_1: () => {
      return rpcRange(additionalFeedData.estimatedCPC);
    },
    custom_label_2: 'gender',
    custom_label_3: () => {
      const merchantId = merchantInfo.id;
      const hasReplacement = merchantId in replacements;

      if (!hasReplacement) {
        let res = '';
        Object.keys(labelMap).forEach(key => {
          if (labelMap[key].includes(placementId)) {
            res = key;
          }
        });
        return res ? res : placementId;
      }

      for (const item of replacements[merchantId]) {
        if (item.check === placementId) return item.replaceWith;
      }

      return placementId;
    },
  };

  return Object.entries(neededAttributes).reduce((acc, [key, path]) => {
    let attrValue;

    if (typeof path === 'string') {
      if (path === 'gender') {
        const genderVal = _.get(offerJson, path);
        attrValue = getGenderValue(genderVal);
      } else attrValue = _.get(offerJson, path);
    } else {
      attrValue = path(offerJson);
    }
    acc[key] = attrValue ?? '';
    return acc;
  }, {});
};
const offerAttributesToString = offer => {
  const headers = Object.values(BingAttributes);
  const headerValues = headers.map(header => offer[header]);
  const joinedHeaderValues = headerValues.join('\t').concat('\n');
  return joinedHeaderValues;
};
const getAdditionalFeedData = merchantId => {
  return new Promise((resolve, reject) => {
    const feedFiles = fs.readdirSync(FEED.META.FEED_PATH);
    const merchantOfferRegex = `${merchantId}_part\\d{3}.json$`;
    const merchantOffers = feedFiles.filter(item => item.match(merchantOfferRegex));
    if (merchantOffers.length < 1) {
      resolve({});
    }
    const offersTracker = merchantOffers.slice();
    const result = [];
    const onDoneOffer = offerName => {
      const index = offersTracker.findIndex(item => item === offerName);
      offersTracker.splice(index, 1);
      if (offersTracker.length < 1) {
        const resultMap = result.reduce((acc, item) => {
          acc[item.id] = item;
          return acc;
        }, {});
        resolve(resultMap);
      }
    };
    for (const offer of merchantOffers) {
      const readStream = fs
        .createReadStream(`${FEED.META.FEED_PATH}/${offer}`)
        .pipe(JSONStream.parse('offers.offer..'));
      readStream.on('data', data => {
        const { id, merchantProductId, estimatedCPC, url, shipType, shipAmount, ecpcMultiplierKey } = data;
        let shipping = '';
        const hasNoShipping = !shipType || shipType?.toLowerCase() === 'unknown';
        const hasNoShipAmount = !shipAmount;
        if (hasNoShipping || hasNoShipAmount) shipping = '';
        else shipping = shipAmount.integral;
        const toAdd = {
          id,
          merchantId,
          merchantProductId,
          estimatedCPC: estimatedCPC.integral,
          url: url.value,
          shipping,
          ecpcMultiplierKey,
        };
        result.push(toAdd);
      });
      readStream.on('error', err => {
        console.error('error with ', offer);
        console.error(err);
        reject();
      });
      readStream.on('end', () => {
        onDoneOffer(offer);
      });
    }
  });
};
const createFeedFile = ({ merchantInfo, sourcePath, destination, feedFile, multiplierContents }) => {
  return new Promise((resolve, reject) => {
    const csvParser = csv({ separator: '\t' });
    const readJsonStream = fs.createReadStream(sourcePath).pipe(csvParser);
    const writeStream = fs.createWriteStream(destination);
    const rejectedProducts = [];
    const headers = Object.values(BingAttributes);
    const headerString = headers.join('\t').concat('\n');
    writeStream.write(headerString);
    const additionalFeedData = feedFile;
    const merchantId = merchantInfo.id;
    const placementIds = placementIdsMap[merchantId] ?? [];
    const placementIdRows = multiplierContents.filter(item => placementIds.includes(item.placementId));
    const averageMultiplier =
      placementIdRows.reduce((acc, item) => acc + Number(item.ecpcMultiplier), 0) / placementIdRows.length;
    readJsonStream.on('data', data => {
      const additionalOfferData = additionalFeedData[data.id];
      if (!additionalOfferData) {
        rejectedProducts.push(data.id);
        return;
      }
      const originalEstimatedCpc = additionalOfferData.estimatedCPC;
      if (!placementIds || placementIds.length === 0) {
        const placementId = '';
        const offerAttr = extractBingAttributes(merchantInfo, data, additionalOfferData, placementId);
        const offerAttrString = offerAttributesToString(offerAttr);
        writeStream.write(offerAttrString);
        return;
      }
      const originalId = data.id;
      for (const index in placementIds) {
        const placementId = placementIds[index];
        const newId = originalId + index;
        data.id = newId;
        const matchPlacement = multiplierContents.filter(item => placementId === item.placementId);
        const matchPlacementAndKey = matchPlacement.find(
          item => item.ecpcMultiplierKey === additionalFeedData.ecpcMultiplierKey
        );
        if (matchPlacementAndKey) {
          const adjustedCpc = originalEstimatedCpc * matchPlacementAndKey.ecpcMultiplier;
          additionalOfferData.estimatedCPC = adjustedCpc;
          const offerAttr = extractBingAttributes(merchantInfo, data, additionalOfferData, placementId);
          const offerAttrString = offerAttributesToString(offerAttr);
          writeStream.write(offerAttrString);
          continue;
        }
        // has a match placement but no matching key
        if (matchPlacement.length) {
          const [firstRow] = matchPlacement;
          const adjustedCpc = originalEstimatedCpc * firstRow.ecpcMultiplier;
          additionalOfferData.estimatedCPC = adjustedCpc;
          const offerAttr = extractBingAttributes(merchantInfo, data, additionalOfferData, placementId);
          const offerAttrString = offerAttributesToString(offerAttr);
          writeStream.write(offerAttrString);
          continue;
        }
        // has no matching placement but other placement ids exist
        if (placementIdRows.length) {
          const adjustedCpc = originalEstimatedCpc * averageMultiplier;
          additionalOfferData.estimatedCPC = adjustedCpc;
          const offerAttr = extractBingAttributes(merchantInfo, data, additionalOfferData, placementId);
          const offerAttrString = offerAttributesToString(offerAttr);
          writeStream.write(offerAttrString);
          continue;
        }
        // if no placement id at all
        const adjustedCpc = originalEstimatedCpc * 0.5;
        additionalOfferData.estimatedCPC = adjustedCpc;
        const offerAttr = extractBingAttributes(merchantInfo, data, additionalOfferData, placementId);
        const offerAttrString = offerAttributesToString(offerAttr);
        writeStream.write(offerAttrString);
      }
    });
    readJsonStream.on('end', () => {
      if (rejectedProducts.length > 0) {
        const data = rejectedProducts.join('\n');
        fs.writeFile(`${FEED.META.ROOT_PATH}/rejected-product-ids-${merchantInfo.id}.txt`, data, err => {
          if (err) throw err;
          console.log('Rejected product IDs saved to file.');
        });
      }
      resolve(1);
    });
    readJsonStream.on('error', () => {
      reject();
    });
  });
};

module.exports = {
  placementIdsMap,
  extractBingAttributes,
  offerAttributesToString,
  getAdditionalFeedData,
  createFeedFile,
};
