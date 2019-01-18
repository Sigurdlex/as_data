const { Transform, } = require('stream');
const fs = require('fs');
const jwt = require('jsonwebtoken');

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

exports.addNewLines = () => new Transform({
  transform(chunk, encoding, done) {
    this.push(chunk.toString().replace(/},/g, '}\n').replace(/^\[|]$/g, ''));
    done();
  }
});