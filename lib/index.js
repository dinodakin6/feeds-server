const fs = require('fs');
const path = require('path');

const integralToUSDString = integral => {
  if (integral == '' || integral == null) return '';

  const decimal = Number(integral) / 100;
  return `${decimal} USD`;
};

const removeQuotes = text => {
  return text.replace(/"/g, '');
};

const readCsvToObjectArray = filePath => {
  const fileContent = fs.readFileSync(filePath, 'utf8');

  const lines = fileContent.split('\n');

  const header = lines[0].trim().split('\t');

  const dataArray = [];

  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].trim().split('\t');

    const obj = {};
    for (let j = 0; j < header.length; j++) {
      obj[header[j]] = values[j];
    }

    dataArray.push(obj);
  }

  return dataArray;
};

const saveTextToFile = ({ data, filePath }) => {
  fs.writeFileSync(path.resolve(__dirname, `../${filePath}`), data, { flag: 'a' });
};

const deleteDirectory = path => {
  fs.rmSync(path, { recursive: true, force: true });
};

const rpcRange = val => {
  const integral = parseInt(val);
  if (integral >= 1 && integral <= 5) {
    return '$0.01 - $0.05';
  } else if (integral >= 5.1 && integral <= 10) {
    return '$0.051 - $0.10';
  } else if (integral >= 10.1 && integral <= 20) {
    return '$0.101 - $0.20';
  } else if (integral >= 20.1 && integral <= 30) {
    return '$0.201 - $0.30';
  } else if (integral >= 30.1 && integral <= 40) {
    return '$0.301 - $0.40';
  } else if (integral >= 40.1 && integral <= 50) {
    return '$0.401 - $0.50';
  } else if (integral >= 50.1 && integral <= 60) {
    return '$0.501 - $0.60';
  } else if (integral >= 60.1 && integral <= 70) {
    return '$0.601 - $0.70';
  } else if (integral >= 70.1 && integral <= 80) {
    return '$0.701 - $0.80';
  } else if (integral >= 80.1 && integral <= 90) {
    return '$0.801 - $0.90';
  } else if (integral >= 90.1 && integral <= 100) {
    return '$0.901 - $1.00';
  } else if (integral >= 100.1 && integral <= 110) {
    return '$1.01 - $1.10';
  } else if (integral > 110) {
    return '$1.101';
  } else if (integral < 1) {
    return '< $0.01';
  }
};

const flog = data => {
  const logPath = path.resolve(__dirname, '../logs/logs.txt');
  const timeNow = new Date().toISOString();

  const toWrite = `[${timeNow}] ${data}\n`;
  console.log(toWrite);

  fs.writeFileSync(logPath, toWrite, { flag: 'a' });
};

module.exports = {
  integralToUSDString,
  removeQuotes,
  readCsvToObjectArray,
  saveTextToFile,
  deleteDirectory,
  rpcRange,
  flog,
};
