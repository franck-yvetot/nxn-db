const debug = require("@nxn/debug")('MySql');

const {configSce} = require('@nxn/boot');
const mysql = require('mysql2/promise');

class MySqlInstance
{
  constructor(config) {
    this.init(config);
}

async init(config) {
  if(!config || this.config)
    return;

      this.config=config;
  this.conPath = config.conPath || '.MySql';
  }

  async connect() {
    if(this.connected)
        return true; 

    // buckets config
    try {            
        let conInfo = configSce.loadConfig(this.conPath);

        if(!conInfo.MYSQL_USER)
            throw "cant find MYSQL_USER for connecting MySql instance";
        if(!conInfo.MYSQL_PWD)
            throw "cant find MYSQL_PWD for connecting MySql instance";
        if(!conInfo.MYSQL_DB)
            throw "cant find MYSQL_DB for connecting MySql instance";
        if(!conInfo.MYSQL_HOST)
            throw "cant find MYSQL_HOST for connecting MySql instance";

        const host = conInfo.MYSQL_HOST;
        const port   = conInfo.MYSQL_PORT || 3306;
        const dbName = conInfo.MYSQL_DB;

        // Database Name
        try {
            // Use connect method to connect to the Server
            this.con = await mysql.createConnection({
                host: host, port:port,
                user: conInfo.MYSQL_USER, password:conInfo.MYSQL_PWD,
                database: dbName
              });

          } catch (err) {
            debug.error(err.stack);
          }
                
        if(this.con)
            debug.log("MySql instance connected on db "+dbName);
        else
            throw "cant connect MySql instance "+dbName;

        this.connected = true;
        return true;
    }
    catch(err) {
        debug.error(`cant connect to MySql instance `+err);
        return Promise.reject({error:500,error:"cant conect to BigQuery "+err});
    }
}  

async close() {
  if(this.connected)
  {
    this.connected = false;
    const con = this.con;
    await con.end();
  }
}

async query(q,values) {
    
    await this.connect();

    let res;
    if(values)
      res = await this.con.execute(q,values);
    else
      res = await this.con.query(q);

    if(res.insertId)
      return res.insertId;

    const [rows, fields] = res;     
    return rows;
  }

  /* ============ SUPPORT INTERFACE UNIFIEE BASEE SUR MONGODB ================= */
  async _mapWhere(query) {
    var where = "";
    if(query)
        objectSce.forEachSync(query, (value,name)=> {
            where += " "+name + "='"+value+"'";
        });
    return where;
}

async _mapLimit(limit=0,skip=0) {
    let s='';
    if(limit) 
    {
        s = " LIMIT "+skip+" "+limit;
    }

    return s;
}

async findOne(query,col) {

    const where = this._mapWhere(query);
    col = col || config.table;
    let qs = this.queries.findOne || "select * from "+col;
    qs += where;

    const docs = await this.db.query(qs);
    if(docs.length>=1)
        return docs[0];
    
    return null;
}

async find(query,col,limit=0,skip=0) {
    const where = this._mapWhere(query);
    col = col || config.table;
    const qlimit = this._mapLimit(limit,skip);

    let qs = this.queries.find || "select * from "+col;
    qs += where;
    qs += qlimit;

    const docs = await this.db.query(qs);
    return docs;
}

async count(query,col,limit=0,skip=0) {
    const where = this._mapWhere(query);
    col = col || config.table;
    const qlimit = this._mapLimit(limit,skip);

    let qs = this.queries.count || "count * from "+col;
    qs += where;
    qs += qlimit;

    const docs = await this.db.query(qs);
    return docs;
}

async insertOne(doc,col) {
    col = col || config.table;

    let qs = this.queries.insertOne || "INSERT INTO "+col;

    let fnames = [];
    let values = [];
    objectSce.forEachSync(doc,(value,name)=> {
        fnames.push(name);
        values.push(value);
    });

    qs += "("+fnames.join(",")+") "+ "VALUES ('"+values.join("','")+"')";

    const insertId= await this.db.query(qs);
    
    return insertId;
}

async insertMany(docs,col) {
    col = col || config.table;
    let qs = this.queries.insertMany || "INSERT INTO "+col;

    if(docs.length == 0)
        return null;
   
    const doc = docs[0];
    let fnames = [];
    objectSce.forEachSync(doc,(value,name)=> {
        fnames.push(name);
    });
   
    let aValues = [];
    docs.forEach(row => {
        let values = [];
        objectSce.forEachSync(row,(value)=> {
            values.push(value);
        });
        aValues.push("('"+values.join("','")+"')");
    });

    let qvals = aValues.join(",");
    qs += " ("+fnames.join(",")+") "+ "VALUES ("+qvals+")";
  
    return await this.db.query(qs);
}

async updateOne(query,doc,col,addIfMissing=true) {

    col = col || config.table;
    let qs = this.queries.updateOne || 
        (addIfMissing ? "REPLACE " : "UPDATE ")+col+" SET ";

    const where = this._mapWhere(query);

    let fnames = [];
    let values = [];
    objectSce.forEachSync(doc,(value,name)=> {
        fnames.push(name);
        values.push(value);
    });

    qs += "("+fnames.join(",")+") "+ "VALUES ('"+values.join("','")+"')";
    qs += where;

    const res = await this.db.query(qs);
    
    return res;
}

async updateMany(query,docs,col,addIfMissing=true) {
    if(docs.length == 0)
        return null;

    const where = this._mapWhere(query);

    col = col || config.table;
    let qs = this.queries.updateMany || 
        (addIfMissing ? "REPLACE " : "UPDATE ") +col+" SET ";
   
    const doc = docs[0];
    let fnames = [];
    objectSce.forEachSync(doc,(value,name)=> {
        fnames.push(name);
    });
   
    let aValues = [];
    docs.forEach(row => {
        let values = [];
        objectSce.forEachSync(row,(value)=> {
            values.push(value);
        });
        aValues.push("('"+values.join("','")+"')");
    });

    let qvals = aValues.join(",");
    qs += " ("+fnames.join(",")+") "+ "VALUES ("+qvals+")";
    qs += where;

    const res = await this.db.query(qs);
    
    return res;
}

async deleteOne(query,col) {
    col = col || config.table;
    let qs = this.queries.deleteOne || "DELETE FROM "+col;
        
    const where = this._mapWhere(query);
    qs += where;

    const res = await this.db.query(qs);
    
    return res;
}

async deleteMany(query,col) {
    col = col || config.table;
    let qs = this.queries.deleteMany || "DELETE FROM "+col;
        
    const where = this._mapWhere(query);
    qs += where;

    const res = await this.db.query(qs);
    
    return res;
}  

}

class MySqlSce
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

    return new MySqlInstance(config)
  }
}
  

module.exports = new MySqlSce();