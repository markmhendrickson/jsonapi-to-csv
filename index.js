const async = require('async');
const fs = require('fs');
const AWS = require('aws-sdk');
const pluralize = require('pluralize');
const json2csv = require('json2csv');
const showdown = require('showdown');
const uniqid = require('uniqid');
const mime = require('mime-types');
const path = require('path');

const csvParser = new json2csv.Parser();
const showdownConverter = new showdown.Converter();

var ranger = require('park-ranger')();

const dataDir = process.env.DATA_DIR;

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_KEY_ID,
  secretAccessKey: process.env.AWS_ACCESS_KEY
});

function loadType(type) {
  var resources;

  if (fs.existsSync(`${dataDir}/${type}`)) {
    let dir = `${dataDir}/${type}`;
    resources = fs.readdirSync(dir).filter((file) => file.endsWith('.json')).map((file) => JSON.parse(fs.readFileSync(`${dir}/${file}`)));
  } else {
    resources = JSON.parse(fs.readFileSync(`${dataDir}/${type}.json`));

    if (resources.items) {
      resources = resources.items;
    }
  }

  async.map(resources, loadResource, (err, resources) => {
    fs.writeFileSync(`${dataDir}/${type}.csv`, csvParser.parse(resources));
  });
};

function loadResource(resource, done) {
  function loadRelationships(done) {
    if (!resource.relationships) { return done(); }

    Object.keys(resource.relationships).forEach((relationship) => {
      if (!Array.isArray(resource.relationships[relationship])) {
        var relatedResource = JSON.parse(fs.readFileSync(`${dataDir}/${resource.relationships[relationship].data.type}/${resource.relationships[relationship].data.id}.json`));
        
        Object.keys(relatedResource.attributes).forEach((property) => {
          resource[`${relationship}-${property}`] = relatedResource.attributes[property];
        });
      }
    });

    done();
  }

  function flattenResource(done) {
    if (!resource.attributes) { return done(); }

    Object.keys(resource.attributes).forEach((attribute) => {
      resource[attribute] = resource.attributes[attribute];
    });

    delete resource.attributes;
    delete resource.relationships;

    done();
  }

  function loadAssets(done) {
    async.each(Object.keys(resource), (property, done) => {
      if (property.endsWith('-url') && !resource[property].startsWith('http')) {
        console.log('uploadFile', `${dataDir}${resource[property]}`);
        uploadFile(`${dataDir}${resource[property]}`).then((location) => {
          console.log('location', location);
          resource[property] = location;
          done();
        }).catch((err) => {
          console.error('Failed to load asset', err);
          done(err);
        });
      } else {
        done();
      }
    }, done);
  }

  function convertDates(done) {
    Object.keys(resource).forEach((property) => {
      if (property.endsWith('At') && Number.isInteger(resource[property])) {
        resource[property] = new Date(resource[property] * 1000)
      }
    });

    done();
  }

  function loadBody(done) {
    if (!resource.body && fs.existsSync(`${dataDir}/${resource.type}/${resource.id}.body.md`)) {
      resource.body = fs.readFileSync(`${dataDir}/${resource.type}/${resource.id}.body.md`, 'utf8');
    }

    done();
  }

  function convertMarkdown(done) {
    Object.keys(resource).forEach((property) => {
      if (property === 'body') {
        resource[property] = showdownConverter.makeHtml(resource[property]);
      }
    });

    done();
  }

  async.series([loadRelationships, flattenResource, loadAssets, loadBody, convertDates, convertMarkdown], (err) => {
    done(err, resource);
  });
};

function uploadFile(filePath) {
  return new Promise((resolve, reject) => {
    try {
      var data = fs.readFileSync(filePath);
    } catch (e) {
      return resolve();
    }

    s3.upload({
      ACL: 'public-read',
      Bucket: process.env.AWS_BUCKET,
      Key: `${uniqid()}${path.extname(filePath)}`,
      Body: data,
      ContentType: mime.lookup(filePath)
    }, (err, data) => {
      if (err) { return reject(err); }
      resolve(data.Location);
    });
  });
}

loadType(process.env.TYPE); // e.g. TYPE=posts npm start