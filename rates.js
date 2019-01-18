const fs = require('fs');
const fetch = require('node-fetch');
const { BigQuery, } = require('@google-cloud/bigquery');
const { format, subDays, } = require('date-fns');
const { promisify, } = require('util');
const ndjson = require('ndjson');
const { Readable } = require('stream');

const { rateKey, } = require('./keys');
const { addNewLines, } = require('./utils');

const bigquery = new BigQuery();

const getCurrencies = async () => {
  const ratesQuery = `SELECT DISTINCT(customer_currency),
  COUNT(*)
  from app_store.SALES1
  GROUP BY customer_currency`;

  const options = {
    query: ratesQuery,
    location: 'US',
  };

  const [rows] = await bigquery.query(options);
  return rows.map(({ customer_currency, }) => customer_currency).join(',');
};

const loadRates = async currencies => {
  const transformQuotes = quotes => Object.entries(quotes).reduce((obj, [key, val]) => ({
    ...obj,
    [key.replace('USD', '')]: val,
  }), {});

  let date = new Date();
  const result = [];
  while(true) {
    const formattedDate = format(date, 'YYYY-MM-DD');
    if (formattedDate === '2018-03-01') break;
    const res = await fetch(`http://apilayer.net/api/historical?access_key=${rateKey}&date=${formattedDate}&currencies=${currencies}`);
    const json = await res.json();
    result.push(json);
    date = subDays(date, 1);
  }
  return result.map(({ date, quotes, }) => ({
    date,
    ...transformQuotes(quotes),
  }));
};

const createTable = async response => {
  const json = JSON.stringify(response);

  const file = fs.createWriteStream('./rates.json');
  const finishPromise = new Promise((resolve, reject) => {
    file.on('finish', resolve);
    file.on('error', reject);
  });
  const inStream = new Readable({
    read() {
      this.push(json);
      this.push(null);
    }
  });
  await inStream.pipe(addNewLines()).pipe(file);
  await finishPromise;
  const [job] = await bigquery
    .dataset('app_store')
    .table('RATES')
    .load('rates.json', {
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
      schema: {
        fields: Object.keys(response[0]).map(name => ({
          name,
          type: name === 'date' ? 'STRING' : 'NUMERIC',
          mode: 'NULLABLE',
        })),
      },
    });
  console.log('bigquery results', job.status);
};

(async () => {
  const currencies = await getCurrencies();
  const response = await loadRates(currencies);
  await createTable(response);
})();



