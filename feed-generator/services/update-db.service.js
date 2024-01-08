const RegenerateHistoryModel = require('../../database/models/regenerate.model');
const { flog } = require('../lib');

const insertRegenerateData = async (id, name, requestId) => {
  try {
    const newData = new RegenerateHistoryModel({
      merchantId: id,
      merchantName: name,
      regenerateRequestDate: new Date(),
      regenerateStatus: 'pending',
      requestId: requestId,
    });

    await newData.save();
    flog(`Successfully inserted data to db for merchant: ${merchantName}`);
  } catch (error) {
    flog(`Error saving data to db ${error}`);
  }
};

const updateRegenerateData = async (id, status, requestId) => {
  try {
    const existingData = await RegenerateHistoryModel.findOne({ merchantId: id, requestId: requestId });

    console.log('existingData >>>', existingData);

    if (!existingData) throw new Error('Regenerate Data not found');

    existingData.regenerateRequestDate = new Date();
    existingData.regenerateStatus = status;

    await existingData.save();
    flog(`Successfully updated data of merchantId: ${id} in DB`);
  } catch (error) {
    flog(`Failed updating data of merchantId: ${id} in DB: ${error}`);
  }
};

module.exports = {
  insertRegenerateData,
  updateRegenerateData,
};
