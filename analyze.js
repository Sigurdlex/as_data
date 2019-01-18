const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const { promisify, } = require('util');

const asyncReadDir = promisify(fs.readdir);

(async () => {
  const csvToJsonPromise = name => new Promise((resolve, reject) => {
    const results = [];
    const date = name.split(/SALES-|\./)[1];
    fs.createReadStream(path.join('./csvs/', name))
      .pipe(csv())
      .on('data', data => results.push(data))
      .on('end', () => resolve({ date, results, }))
      .on('error', reject);
  });

  const getCountriesStat = data => data.reduce((obj, cur) => {
    const {
      'Country Code': country,
      'Customer Currency': currency,
      'Customer Price': price,
    } = cur;
    return {
      ...obj,
      [country]: {
        currency,
        amount: obj[country] ? obj[country].amount + 1 : 1,
        sum: obj[country] ? obj[country].sum + +price : +price,
        avg: Math.round((obj[country] ? (obj[country].sum + +price) / (obj[country].amount + 1) : +price) * 100) / 100,
      }
    }
  }, {});

  const files = await asyncReadDir('./csvs');
  const promises = files
    .filter(name => name.startsWith('SALES'))
    .map(name => csvToJsonPromise(name));
  const json = await Promise.all(promises);
  const result = json.reduce((obj, { date, results, }) => ({
    ...obj,
    [date]: getCountriesStat(results),
  }), {});
  await fs.writeFile('./calc/dataByDate.json', JSON.stringify(result));
})();