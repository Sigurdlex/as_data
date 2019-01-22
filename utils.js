const { Transform, } = require('stream');
const fs = require('fs');
const jwt = require('jsonwebtoken');
const { format, parse, } = require('date-fns');

const { iss, kid, } = require('./keys');

exports.tokenGen = () => {
  const cert = fs.readFileSync('AuthKey.p8');
  const payload = {
    iss,
    exp: Math.floor(Date.now() / 1000) + (20 * 60),
    aud: "appstoreconnect-v1"
  };
  const header = {
    alg: "ES256",
    kid,
    typ: "JWT"
  };

  console.log('generating app store connect jwt');
  return jwt.sign(payload, cert, { header, });
};

exports.tsvToCsv = () => new Transform({
  transform(chunk, encoding, done) {
    this.push(chunk.toString().replace(/\t/g, ','));
    done();
  }
});

exports.addUsdPrice = (type, rates, formattedDate) => new Transform({
  transform(chunk, encoding, done) {
    const obj = JSON.parse(chunk.toString());
    switch (type) {
      case 'SUBSCRIBER': {
        const {
          'Customer Price': customer_price,
          'Customer Currency': customer_currency,
          'Developer Proceeds': developer_proceeds,
          'Proceeds Currency': currency_of_proceeds,
          'Event Date': event_date,
        } = obj;
        const day = rates.find(({ date, }) => date === format(parse(event_date), 'YYYY-MM-DD'));
        const result = {
          ...obj,
          customer_price_in_usd: Math.round(customer_price / day[customer_currency] * 100) / 100,
          developer_proceeds_in_usd: currency_of_proceeds
            ? Math.round(developer_proceeds / day[currency_of_proceeds] * 100) / 100
            : 0,
        };
        this.push(JSON.stringify(result));
        break;
      }
      case 'SALES': {
        const {
          'Customer Price': customer_price,
          'Customer Currency': customer_currency,
          'Developer Proceeds': developer_proceeds,
          'Currency of Proceeds': currency_of_proceeds,
          'Begin Date': begin_date,
        } = obj;
        const day = rates.find(({ date, }) => date === format(parse(begin_date), 'YYYY-MM-DD'));
        const result = {
          ...obj,
          customer_price_in_usd: Math.round(customer_price / day[customer_currency] * 100) / 100,
          developer_proceeds_in_usd: currency_of_proceeds
            ? Math.round(developer_proceeds / day[currency_of_proceeds] * 100) / 100
            : 0,
        };
        this.push(JSON.stringify(result));
        break;
      }
      case 'SUBSCRIPTION': {
        const {
          'Customer Price': customer_price,
          'Customer Currency': customer_currency,
          'Developer Proceeds': developer_proceeds,
          'Proceeds Currency': currency_of_proceeds,
        } = obj;
        const day = rates.find(({ date, }) => date === formattedDate);
        const result = {
          ...obj,
          event_date: formattedDate,
          customer_price_in_usd: Math.round(customer_price / day[customer_currency] * 100) / 100,
          developer_proceeds_in_usd: currency_of_proceeds
            ? Math.round(developer_proceeds / day[currency_of_proceeds] * 100) / 100
            : 0,
        };
        this.push(JSON.stringify(result));
        break;
      }
      default: {
        this.push(chunk);
      }
    }
    done();
  }
});

exports.fileWriteStreamPromise = file => new Promise((resolve, reject) => {
  file.on('finish', resolve);
  file.on('error', reject);
});