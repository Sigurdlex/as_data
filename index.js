const fs = require('fs');
const zlib = require('zlib');
const fetch = require('node-fetch');
const { BigQuery, } = require('@google-cloud/bigquery');
const { format, subDays, lastDayOfWeek, } = require('date-fns');

const { tsvToCsv, tokenGen, } = require('./utils');
const schema = require('./schema');

const reportTypes = ['SALES', 'SUBSCRIPTION', 'SUBSCRIPTION_EVENT'];

const download = async type => {
  const projectId = 'impressive-tome-227410';
  const bigquery = new BigQuery({ projectId, });
  const datasetId = 'app_store';

  let date = new Date();
  if (type === 'SALES') {
    date = lastDayOfWeek(new Date(), { weekStartsOn: 1, });
  }
  const daysSub = type === 'SALES' ? 7 : 1;
  let token = tokenGen();
  while(true) {
    const formatedDate = format(date, 'YYYY-MM-DD');
    const fileName = `./csvs/${type}-${formatedDate}.csv`;
    if (fs.existsSync(fileName)) {
      date = subDays(date, daysSub);
      continue;
    }

    const frequency = type === 'SALES' ? 'WEEKLY' : 'DAILY';
    const reportSubType = type === 'SALES' ? 'OPT_IN' : 'SUMMARY';
    const version = type === 'SALES' ? '1_0' : '1_1';

    const response = await fetch(`https://api.appstoreconnect.apple.com/v1/salesReports?filter[frequency]=${frequency}&filter[reportSubType]=${reportSubType}&filter[reportType]=${type}&filter[vendorNumber]=87808941&filter[version]=${version}&filter[reportDate]=${formatedDate}`, {
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
        date = subDays(date, daysSub);
        continue;
      }
    }
    date = subDays(date, daysSub);
    const file = fs.createWriteStream(fileName);
    const finishPromise = new Promise((resolve, reject) => {
      file.on('finish', resolve);
      file.on('error', reject);
    });

    await body
      .pipe(zlib.createGunzip())
      .pipe(tsvToCsv())
      .pipe(file);

    await finishPromise;

    const [job] = await bigquery
      .dataset(datasetId)
      .table(type)
      .load(fileName, {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        schema: { fields: schema[type], },
        headers: {},
      });
    console.log('bigquery results', job.status);
  }
};

(async () => {
  try {
    await download(reportTypes[0]);
    await download(reportTypes[1]);
    console.log('that\'s all!')
  } catch(err) {
    console.log(err);
  }
})();