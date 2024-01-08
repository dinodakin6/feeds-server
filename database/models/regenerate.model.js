const connection = require('../db');
const mongoose = require('mongoose');

const schema = mongoose.Schema(
  {
    merchantId: String,
    merchantName: {
      type: String,
      default: '',
    },
    regenerateRequestDate: Date,
    regenerateStatus: String, // pending, regenerating, uploading, done
    requestId: String,
  },
  { timestamps: true }
);

const model = connection.model('RegenerateHistory', schema);

module.exports = model;
