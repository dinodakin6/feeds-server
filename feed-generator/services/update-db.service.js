const RegenerateHistoryModel = require('../../database/models/regenerate.model');
const { flog } = require('../lib');

const insertRegenerateData = async (id, name) => {
  try {
    const newData = new RegenerateHistoryModel({
      merchantId: id,
      merchantName: name,
      regenerateRequestDate: new Date(),
      regenerateStatus: 'pending',
    });

    await newData.save();
    flog(`Successfully inserted data to db for merchant: ${merchantName}`);
  } catch (error) {
    flog(`Error saving data to db ${error}`);
  }
};

const updateRegenerateData = async (id, status) => {
  try {
    const existingData = await RegenerateHistoryModel.find({ merchantId: id })
      .sort({ regenerateRequestDate: -1 })
      .limit(1);

    console.log('existingData >>>', existingData);

    if (!existingData) throw new Error('Regenerate Data not found');

    const latestData = existingData[0];

    latestData.regenerateRequestDate = new Date();
    latestData.regenerateStatus = status;

    await latestData.save();
    flog(`Successfully updated data of merchantId: ${id} in DB`);
  } catch (error) {
    flog(`Failed updating data of merchantId: ${id} in DB: ${error}`);
  }
};

module.exports = {
  insertRegenerateData,
  updateRegenerateData,
};
