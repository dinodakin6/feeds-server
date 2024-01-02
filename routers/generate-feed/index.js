const express = require('express');
const router = express.Router();
const { enqueueRequest } = require('../../feed-generator/services/generate-feed.service');

router.post('/', async (req, res, next) => {
  try {
    const { merchantId, merchantName } = req.body;

    if (!merchantId || !merchantName)
      return res.status(400).json({
        message: 'Merchant ID and Merchant Name not found',
      });

    enqueueRequest(merchantName, merchantId);

    return res.sendStatus(200);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
