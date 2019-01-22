const fs = require('fs');
const fetch = require('node-fetch');
const { BigQuery, } = require('@google-cloud/bigquery');
const { format, subDays, } = require('date-fns');
const { promisify, } = require('util');
const { Readable, } = require('stream');
const { Transform: Json2csvTransform, } = require('json2csv');

const { rateKey, } = require('./keys');
const { fileWriteStreamPromise, } = require('./utils');
const { RATES: ratesSchema, } = require('./schema');

const projectId = 'impressive-tome-227410';
const bigquery = new BigQuery({ projectId, });
const datasetId = 'app_store';
const asyncReadFile = promisify(fs.readFile);
const asyncWriteFile = promisify(fs.writeFile);

const writeJson = async () => {
  const currencies = 'BRL,EGP,DKK,HRK,MYR,SAR,EUR,HUF,PHP,TRY,THB,NGN,CAD,KRW,RUB,PKR,JPY,HKD,PLN,IDR,CHF,QAR,USD,MXN,VND,ILS,NOK,INR,AUD,CLP,PEN,KZT,CZK,NZD,RON,CNY,SGD,TZS,ZAR,SEK,TWD,BGN,GBP,AED,COP';
  const transformQuotes = quotes => Object.entries(quotes).reduce((obj, [key, val]) => ({
    ...obj,
    [key.replace('USD', '')]: val,
  }), {});

  const json = await asyncReadFile('rates.json', 'utf8');
  const prevData = JSON.parse(json);

  const latestDate = prevData[0].date;
  console.log('latest date', latestDate);

  let date = new Date();
  const newData = [];
  while(true) {
    const formattedDate = format(date, 'YYYY-MM-DD');
    console.log('formatted date', formattedDate);
    if (formattedDate === latestDate) break;
    const res = await fetch(`http://apilayer.net/api/historical?access_key=${rateKey}&date=${formattedDate}&currencies=${currencies}`);
    const json = await res.json();
    newData.push(json);
    date = subDays(date, 1);
  }

  console.log('newData', newData);
  if (newData.length) {
    const formattedData = newData
      .map(({ date, quotes, }) => ({ date, ...transformQuotes(quotes), }));
    const totalData = formattedData.concat(prevData);

    await asyncWriteFile('./rates.json', JSON.stringify(totalData));
    const dataForCsv = formattedData.reduce((arr, cur) => {
      const newPortion = Object.keys(cur)
        .filter(name => name !== 'date')
        .map(name => ({ date: cur.date, currency: name, rate: cur[name], }));
      return [...arr, ...newPortion];
    }, []);

    const readableStream = obj => new Readable({
      read() {
        this.push(JSON.stringify(obj));
        this.push(null);
      }
    });
    const json2csv = new Json2csvTransform({ quote: '' });
    const file = fs.createWriteStream('./rates.csv');
    const filePromise = fileWriteStreamPromise(file);
    readableStream(dataForCsv)
      .pipe(json2csv)
      .pipe(file);
    await filePromise;
    const [job] = await bigquery
      .dataset(datasetId)
      .table('RATES')
      .load('./rates.csv', {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        schema: { fields: ratesSchema, },
      });
    console.log('bigquery results for rates', job.status);
  }
};

const loadRates = async () => {
  await writeJson();
  console.log('last rates have been written to rates.json');
};

module.exports = loadRates;



