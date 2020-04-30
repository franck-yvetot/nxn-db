const debug = require("nxn-boot/debug.service")('MySql');

const {configSce} = require('nxn-boot');
const mysql = require('mysql2/promise');

class MySqlInstance
{

  constructor(config,conPath) {
      this.config=config;
      this.conPath = conPath;
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

    const [rows, fields] = res;     
    return rows;
  }
}

class MySqlSce
{
  constructor() {
      this.config = {};
  }

  // if init by boot.service, get a config
  init(config,app,express) {
      this.config = config;
  }

  getInstance(name) {
    let config = {};

    if(this.config.instances && this.config.instances[name])
        config = this.config.instances[name];
    else
        config = this.config;

    let conPath = __clientDir;
    conPath += config.conPath || this.config.conPath || '.MySql.json';

    return new MySqlInstance(config,conPath)
  }
}
  

module.exports = new MySqlSce();