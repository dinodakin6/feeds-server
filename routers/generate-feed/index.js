const express = require('express');
const router = express.Router();
const { regenerateFeed } = require('../../services/generate-feed.service');

router.post('/', (req, res, next) => {
  try {
    const { merchantId, merchantName } = req.body;

    if (!merchantId || !merchantName)
      return res.status(400).json({
        message: 'Merchant ID and Merchant Name not found',
      });

    regenerateFeed(merchantName, merchantId);

    return res.sendStatus(200);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
