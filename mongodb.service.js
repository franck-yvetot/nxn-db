const debug = require("@nxn/debug")('MongoDb');

const {configSce} = require('@nxn/boot');
const MongoClient = require('mongodb').MongoClient;

class MongoDbInstance
{

  constructor(config) {
      this.init(config);
  }

  async init(config) {
    if(!config || this.config)
      return;

      this.config=config;
    this.conPath = config.conPath || '.mongodb.json';
  }

  async connect() {
    if(this.connected)
        return true; 

    // buckets config
    try {            
        let conInfo = configSce.loadConfig(this.conPath);

        if(!conInfo.MONGO_USER)
            throw "cant find MONGO_USER for connecting MongoDb instance";
        if(!conInfo.MONGO_PWD)
            throw "cant find MONGO_PWD for connecting MongoDb instance";
        if(!conInfo.MONGO_DB)
            throw "cant find MONGO_DB for connecting MongoDb instance";
        if(!conInfo.MONGO_DOMAIN)
            throw "cant find MONGO_DOMAIN for connecting MongoDb instance";

        const domain = conInfo.MONGO_DOMAIN;
        const port   = conInfo.PORT || 27017;
        const dbName = conInfo.MONGO_DB;
        const url = 'mongodb://'+domain+':'+port+'/'+dbName;

        // Database Name
        const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true  });
        try {
            // Use connect method to connect to the Server
            await client.connect();

            this.client = client;
            this.db = client.db(dbName);

          } catch (err) {
            debug.error(err.stack);
          }
                
        if(this.db)
            debug.log("MongoDb instance connected on project "+dbName);
        else
            throw "cant connect MongoDb instance "+dbName;

        this.connected = true;
        return true;
    }
    catch(err) {
        debug.error(`cant connect to MongoDb instance `+err);
        return Promise.reject({error:500,error:"cant conect to BigQuery "+err});
    }
}  

  close() {
    this.client.close();
    this.db = this.client = null;
  }

  async collection(col) {
    
    await this.connect();

    return this.db.collection(col);
  }

  async findOne(query,col) {
    await this.connect();

    const doc = await this.db.collection(col).findOne(query,{});
    
    return doc;
  }

  async find(query,col,limit=0,skip=0) {
    await this.connect();

    limit = parseInt(limit);
    const docs = await this.db.collection(col).find(query,{skip:skip,limit:limit}).toArray();
    
    return docs;
  }

  async count(query,col,limit=0,skip=0) {
    await this.connect();

    limit = parseInt(limit);
    const n = await this.db.collection(col).countDocuments(query);
    
    return n;
  }
  
  async insertOne(doc,col) {
    await this.connect();

    const r = await this.db.collection(col).insertOne(doc);
    
    return r.insertedCount;
  }

  async insertMany(docs,col) {
    await this.connect();

    const r = await this.db.collection(col).insertMany(docs);

    return r.insertedCount;
  }

  async updateOne(query,update,col,addIfMissing=true) {
    await this.connect();

    const r = await this.db.collection(col).updateOne(query,{$set:update},{upsert: addIfMissing});
    
    return r.modifiedCount;
  }

  async updateMany(query,update,col,addIfMissing=true) {
    await this.connect();

    const r = await this.db.collection(col).updateMany(query,{$set:update},{upsert: addIfMissing});

    return r.modifiedCount;
  }

  async deleteOne(query,col) {
    await this.connect();

    const r = await this.db.collection(col).deleteOne(query);
    
    return r.deletedCount;
  }
  async deleteMany(query,col) {
    await this.connect();

    const r = await this.db.collection(col).deleteMany(query);
    
    return r.deletedCount;
  }

}

class MongoDbSce
{
  constructor() {
      this.config = {};
  }

  // if init by boot.service, get a config
  init(config) {
      this.config = config;
  }

  getInstance(name) {
    let config = {};

    if(this.config.instances && this.config.instances[name])
        config = this.config.instances[name];
    else
        config = this.config;

    return new MongoDbInstance(config)
  }
}
  

module.exports = new MongoDbSce();