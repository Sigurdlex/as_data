const fs = require('fs');
const zlib = require('zlib');
const fetch = require('node-fetch');
const { BigQuery, } = require('@google-cloud/bigquery');
const { format, subDays, } = require('date-fns');
const csv = require('csvtojson');
const { Transform: Json2csvTransform, } = require('json2csv');
const { promisify, } = require('util');

const { tsvToCsv, tokenGen, addUsdPrice, fileWriteStreamPromise, } = require('./utils');
const schema = require('./schema');
const loadRates = require('./rates');

const asyncReadFile = promisify(fs.readFile);

const reportTypes = ['SUBSCRIPTION_EVENT', 'SUBSCRIPTION', 'SALES', 'SUBSCRIBER'];

const download = async type => {
  const projectId = 'impressive-tome-227410';
  const bigquery = new BigQuery({ projectId, });
  const datasetId = 'app_store';

  let date = new Date();
  let token = tokenGen();
  const rates = JSON.parse(await asyncReadFile('./rates.json'));
  while(true) {
    const formatedDate = format(date, 'YYYY-MM-DD');
    const fileName = `./reports/${type}-${formatedDate}.csv`;
    if (fs.existsSync(fileName)) {
      date = subDays(date, 1);
      continue;
    }

    const version = type === 'SALES' ? '1_0' : '1_1';
    const reportSubType = type === 'SUBSCRIBER' ? 'DETAILED' : 'SUMMARY';

    const response = await fetch(`https://api.appstoreconnect.apple.com/v1/salesReports?filter[frequency]=DAILY&filter[reportSubType]=${reportSubType}&filter[reportType]=${type}&filter[vendorNumber]=87808941&filter[version]=${version}&filter[reportDate]=${formatedDate}`, {
      method: 'GET',
      headers: { 'Authorization': `Bearer ${token}`, },
    });
    const { body, status, } = response;
    console.log('app store connect results', formatedDate, type, status);

    if (status < 200 || status >= 300) {
      const { errors, } = await response.json();
      errors.forEach(err => console.log('app store connect error:', err));
      if (status === 410) {
        break;
      } else if(status === 401) {
        token = tokenGen();
        continue
      } else {
        date = subDays(date, 1);
        continue;
      }
    }
    date = subDays(date, 1);
    const file = fs.createWriteStream(fileName);
    const finishPromise = fileWriteStreamPromise(file);

    const json2csv = new Json2csvTransform({ quote: ''});
    await body
      .pipe(zlib.createGunzip())
      .pipe(tsvToCsv())
      .pipe(csv())
      .pipe(addUsdPrice(type, rates, formatedDate))
      .pipe(json2csv)
      .pipe(file);

    await finishPromise;

    const [job] = await bigquery
      .dataset(datasetId)
      .table(`${type}`)
      .load(fileName, {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        schema: { fields: schema[type], },
      });
    console.log('bigquery results', job.status);
  }
};

(async () => {
  try {
    await loadRates();
    await download(reportTypes[0]);
    await download(reportTypes[1]);
    await download(reportTypes[2]);
    console.log('that\'s all!')
  } catch(err) {
    console.log(err);
  }
})();