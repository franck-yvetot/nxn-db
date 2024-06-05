// @ts-check
const debug = require("@nxn/debug")('MYSQL_SCE');
const {configSce, FlowNode} = require('@nxn/boot');
const {objectSce} = require("@nxn/ext");
const mapper = require("@nxn/ext/map.service");

const mysql = require('mysql2/promise');

const mysql2 = require('mysql2');

const {SchemaField,SchemaFieldEnum,DbView,DbModelInstance,DbModel,DbModelSce} = require("./db_model.service")
module.exports.DbModelSce = DbModelSce;

/** 
 * @typedef {import("nxn_db").TMySQLFindOptions} TMySQLFindOptions 
 * @typedef {import("nxn_db").TMySQLResult} TMySQLResult 
 * */

let pools = {};

// MySQL Handler :  base class for connection and connection pool versions
class MySqlHandlerBase
{
    // process connection config and create parameters for MySQL
    buildParams(conInfo,isPool=false) 
    {
        if(!conInfo.MYSQL_USER)
            throw "cant find MYSQL_USER for connecting MySql instance";

        if(!conInfo.MYSQL_PWD)
            throw "cant find MYSQL_PWD for connecting MySql instance";

        if(!conInfo.MYSQL_DB)
            throw "cant find MYSQL_DB for connecting MySql instance";

        let database = conInfo.MYSQL_DB;

        let params = 
        {
            user: conInfo.MYSQL_USER, 
            password:conInfo.MYSQL_PWD,
            database
        };

        let message = " ";

        // for cloud run
        if(conInfo.MYSQL_SOCKET_PATH)
        {
            // e.g. '/cloudsql/project:region:instance'
            // ex. '/cloudsql/presence-talents:europe-west1:presence-talents-preprod56'
            params.socketPath = conInfo.MYSQL_SOCKET_PATH;
            message += " SOCKET "+params.socketPath;
        }
        else
        {
            if(!conInfo.MYSQL_HOST)
                throw "cant find MYSQL_HOST for connecting MySql instance";
            
            params.host = conInfo.MYSQL_HOST;
            params.port = conInfo.MYSQL_PORT || 3306;

            message += " Host "+params.host+":"+params.port;
        }

        if(conInfo.MYSQL_TIMEOUT) 
        {
            params.connectTimeout = conInfo.MYSQL_TIMEOUT;
            // params.waitForConnections = true;                
        }

        if(isPool)
            params.connectionLimit = conInfo.MYSQL_MAX_CONNECTIONS || 100;

        params.debug = conInfo.MYSQL_DEBUG || false;        
        
        return {params,message};
    }

    // execute the query ("execute" for INSERT/UPDATE/REPLACE/PATCH, "query" for SELECT/DELETE)
    // manage reuse of same connection for "SELECT SQL_CALC_FOUND_ROWS" (nb of rows is kept in con object by MySQL)
    // for that : callback is called if provided, with the con object
    async query(q,view=null,values=null,reconnect=false,cb=null,con=null)
    {
        let res;
        try 
        {
            if(!con)
                con = await this.getCon();
            
            if(values)
                res = await con.execute(q,values);
            else
                res = await con.query(q);

            if(cb)
            {
                // use cb to exec another query on same con
                let p = cb(res,con);
                if(p.then)
                    await p;
            }                
        } 
        catch (error) 
        {
            debug.error("ERROR in MYSQL query "+q);
            debug.error("ERROR message "+error.message);

            if(view)
                debug.error("VIEW ",view.infos());

            throw error;
        }  
        finally 
        {
            this.releaseCon(con);
        }          
    
        if(res.insertId)
            return res.insertId;

        const [rows, fields] = res;     
        return rows;
    }

}

// MySQL Handler :  pooled version
class MySqlPool extends MySqlHandlerBase
{
    constructor() 
    {
        super();
        this.conInfo = null;
        this.connected = false;
    }

    async connect(conInfo, force=false) 
    {
        if(!force && this.connected)
            return this.con; 

        try 
        {            
            let {params,message} = this.buildParams(conInfo,true);

            // Database Name
            try 
            {
                // Use connect method to connect to the Server
                this.pool = mysql.createPool(params);
            } 
            catch (err) 
            {
                debug.error(err.stack);
            }
                    
            if(this.pool)
                debug.log("MYSQL POOL CONNECTED TO DB: "+params.database+message);
            else
                throw "MYSQL POOL CANT CONNECT TO DB :"+params.database+message;

            this.connected = true;
            return this.pool;
        }
        catch(err) 
        {
            debug.error(`cant connect to MySql pool instance `+err);
            return Promise.reject({error:500,error:"cant conect to MySql "+err});
        }
    }

    async getCon() 
    {
        let con = await this.pool.getConnection();        
        // debug.log("get CON "+con.connection._internalId);
        return con;
    }

    async releaseCon(con) {
        if(con)
        {
            // debug.log("Release CON "+con.connection._internalId);
            con.release();    
        }
    }

    async close() {

    }    
}

// MySQL Handler :  simple connection version
// NB. this version is less efficient than pooled version
class MySqlCon extends MySqlHandlerBase
{
    constructor() 
    {
        super();
        this.conInfo = null;
        this.connected = false;
        this.con = null;
    }

    async connect(conInfo, force=false) 
    {
        if(!force && this.connected)
            return this.con;

        try 
        {     
            let {params,message} = this.buildParams(conInfo,true);

            // Database Name
            try 
            {
                // Use connect method to connect to the Server
                this.con = await mysql.createConnection(params);
            } 
            catch (err) 
            {
                debug.error(err.stack);
            }
                    
            if(this.con)
                debug.log("MYSQL CONNECTED TO DB: "+params.database+message);
            else
                throw "MYSQL CANT CONNECT TO DB :"+params.database+message;

            this.connected = true;
            return this.con;
        }
        catch(err) 
        {
            debug.error(`cant connect to MySql instance `+err);
            return Promise.reject({error:500,error:"cant conect to MySql "+err});
        }
    }

    async getCon() 
    {
        return this.con;
    }    

    async releaseCon(con) {
    }

    async close() 
    {
        if(this.connected)
        {
            this.connected = false;
            const con = this.con;
            await con.end();
        }
    }    
}

// MySQL Handler :  simple connection version
// NB. this version is less efficient than pooled version
class MySqlConNoPromise extends MySqlHandlerBase
{
    constructor() 
    {
        super();
        this.conInfo = null;
        this.connected = false;
        this.con = null;
    }

    async connect(conInfo, force = false) {
        if (!force && this.connected) {
            return this.con;
        }
    
        return new Promise((resolve, reject) => {
            try {
                const { params, message } = this.buildParams(conInfo, true);
    
                this.con = mysql2.createConnection(params);
    
                this.con.connect((err) => {
                    if (err) {
                        debug.error("Can't connect to MySql " + err);
                        reject({ error: 500, message: "Can't connect to MySql " + err });
                        return;
                    }
    
                    debug.log("MYSQL CONNECTED TO DB: " + params.database + message);
                    this.connected = true;
                    resolve(this.con);
                });
            } catch (err) {
                debug.error("Can't connect to MySql instance " + err);
                reject({ error: 500, message: "Can't connect to MySql " + err });
            }
        });
    }   

    async getCon() 
    {
        return this.con;
    }    

    async releaseCon(con) {
    }

    async close() 
    {
        if(this.connected)
        {
            this.connected = false;
            const con = this.con;
            await con.end();
        }
    }    


    // execute the query ("execute" for INSERT/UPDATE/REPLACE/PATCH, "query" for SELECT/DELETE)
    // manage reuse of same connection for "SELECT SQL_CALC_FOUND_ROWS" (nb of rows is kept in con object by MySQL)
    // for that : callback is called if provided, with the con object
    async query(q,view=null,values=null,reconnect=false,cb=null,con=null)
    {
        let res;
        let mustRelease = false;
        try 
        {
            if(!con)
            {
                con = await this.getCon();
                mustRelease = true;
            }

            return new Promise((resolve, reject) => {                
            
                if(values)
                    ; // res = await con.execute(q,values);
                else
                {
                    con.query(q, (err, results, fields) => 
                    {
                        if (err) 
                        {
                            debug.error("ERROR in MYSQL query "+q);
                            debug.error("ERROR message "+err.sqlMessage);
                
                            if(view)
                                debug.error("VIEW ",view.infos());

                            if(mustRelease)
                                this.releaseCon(con);
                                                
                            reject(err);
                        }

                        res = results;

                        if(res?.insertId)
                            resolve(res.insertId);

                        if(mustRelease)
                            this.releaseCon(con);
                            
                        resolve(results);
                    }); 
                }
            });
        } 
        catch (error) 
        {
            throw error;
        }  
        finally 
        {
        }    
    }    
}

class MySqlInstance extends FlowNode
{
    /** @type {import("../../clients/services/clientManager.service").IClientManager} */
    clientManager; // injection

    /**
     * collection name and database name of a collection.
     * Uses collection base name and database, using client_id mapping.
     * 
     *  @type {Record<string,{colName,dbName}>} 
     * */
    _collectionsMap = {}

    constructor(inst) {
        super(inst);
    } 

    async init(config,ctxt,...injections)
    {
        if(!config || this.config)
            return;

        super.init(config,ctxt,injections,true);

        this.conHandler = null;

        this.config = config;
        this.conPath = config.conPath || '.MySql';

        this.secretManager = this.getInjection("secrets");
        this.secretId = this.config.secret_id || "mysql";
    }

    async loadConInfo() 
    {
        if(this.conInfo)
            return this.conInfo;

        try 
        {   
            let conInfo;

            if(this.secretManager)
            {
                this.conInfo = await this.secretManager.getEnv(this.secretId);
            }
            else
            {
                this.conInfo = configSce.loadConfig(this.conPath);
            }

            // overload DB name (used for sharing a secret config on several dbs)
            if(this.config.database)
                this.conInfo.MYSQL_DB = this.config.database;
        }   
        catch(err) {
            throw err;
        }

        return this.conInfo;
    }

    async connect(force=false) 
    {
        if(!force && this.conHandler)
            return this.conHandler; 

        try 
        {            
            let conInfo = await this.loadConInfo();

            let isPool = !(conInfo.MYSQL_POOL===false);
            let noPromise = this.config.no_promise || false;

            if(!this.conHandler)
            {
                if(noPromise)
                    this.conHandler = new MySqlConNoPromise()
                else if(isPool)
                    this.conHandler = new MySqlPool();
                else
                    this.conHandler = new MySqlCon();
            }

            await this.conHandler.connect(conInfo,force);
            return this.conHandler;
        }
        catch(err) 
        {
            throw err;
        }        
    }

    async close()
    {
        if(this.conHandler)
        {
            this.conHandler.close();
            this.conHandler = null;
        }
    }

    async query(q,view=null,values=null,reconnect=false,cb=null,con=null) 
    {
        try 
        {
            let conHandler = await this.connect(reconnect);

            return await conHandler.query(q,view,values,reconnect,cb,con);
        }
        catch (error) 
        {
            await this._processError(error,q,view,values,cb,con);

            throw error;
        }
    }

  /* ============ PRIVATE METHODS ================= */

  async _processError(error,q,view,values,cb,con) 
  {
      if(error.message == "Can't add new command when connection is in closed state" 
        || error.code == 'ETIMEDOUT'
        || error.code == 'PROTOCOL_CONNECTION_LOST'
        || error.code == 'EPIPE')
      {
          debug.error("try reconnecting...");
          return this.query(q,view,values,true,cb);
      }
      if(error.code == 'ECONNABORTED')
      {
          debug.error("MYSQL Connection error, reconnecting...");
          return this.query(q,view,values,false,cb);
      }                         
      if(error.code == 'ER_BAD_FIELD_ERROR')
      {
          debug.error("try adding new field...");
          const isOk = await this._fixMissingField(error,view);
          if(isOk)
              return this.query(q,view,values,false,cb,con);
      }
      if(error.code == 'ER_NO_SUCH_TABLE')
      {
          debug.error("try adding new table...");
          const isOk = await this._fixMissingTable(error,view);
          if(isOk)
              return this.query(q,view,values,false,cb,con);
      }
      
      debug.error("Other MYSQL Error (not managed by MYSQLSce) "+error.code);
      throw error;
    }    

    /**
     * 
     * @param {Record<string,any>} query 
     * @param {DbView} view 
     * @param {boolean} withTablePrefix 
     * @returns 
     */
    _mapWhere(query,view,withTablePrefix=true) 
    {
        var where = "";

        const schema = view.schema();

        if(query)
        {
            const prefix = schema.fieldPrefix();

            // support _all_ clause to be added regarless of query params
            query._all_ = "*";

            let aWhere = [];
            objectSce.forEachSync(query, (value,name)=> 
            {
                const fw = view.getFieldWhere(name,value,withTablePrefix);
                if(fw)
                    aWhere.push(fw);
                // where += " "+prefix+name + "='"+value+"'";
            });

            if(aWhere.length)
                where = "WHERE "+aWhere.join(" AND ");
            else
                where = "";
        }

        return where;
    }

    // map field OP value
    fieldWhere(fname,operator='=',valueStr='$value') 
    {
        return fname +" "+ operator +" "+valueStr;
    }

    /**
     * create limit clause
     * 
     * @param {*} limit 
     * @param {*} skip 
     * @returns 
     */
    _mapLimit(limit=0,skip=0) 
    {
        let s='';
        if(limit) 
        {
            s = " LIMIT "+skip+","+limit;
        }

        return s;
    }

    /**
     * 
     * @param {DbView} view 
     * @param {string} viewName 
     * @param {string} defaultQuery 
     * @param {any} map 
     * @returns 
     */
    _buildQuery(view, viewName,defaultQuery,map) {
        let qs = (view && view.getQuery(viewName)) || (this.config.queries && this.config.queries[viewName]) 
        || defaultQuery;

        const qs2 = qs.replace(/%([a-zA-Z_][a-zA-Z0-9_]+)%/g, (match,p1) => { 
            return map[p1];
        })
        
        return qs2;
    }

    /**
     * 
     * @param {*} rec 
     * @param {string} view 
     * @param {boolean} forceEnumLabel 
     * @returns 
     */
    _formatRecord(rec,view,forceEnumLabel=false) {
        const format = view.getFieldsFormats();
        const locale = view.locale();
        const schema = view.schema();

        if(!format)
            return  Object.assign({}, rec);
        
        objectSce.forEachSync(format,(vcsv,k) => 
        {
            if(!vcsv?.split)
                return;

            let av = vcsv?.split(',') || [];
            for (let i=av.length-1;i>=0;i--)
            {
                // execute in reverse order
                let v = av[i].trim();
                const func = "_format_"+v;
                const fdesc = view.field(k);
                if(typeof this[func] == "function")
                    this[func](k,rec,fdesc,locale,forceEnumLabel);
            }
        });

        return rec;
    }

    /**
     * format value to json from text
     * @param {*} fname 
     * @param {*} rec 
     * @param {*} fdesc 
     * @param {*} locale 
     * @param {*} forceEnumLabel 
     */
    _format_json(fname,rec,fdesc,locale,forceEnumLabel=false) 
    {
        if(rec[fname])
        {
            rec[fname] = JSON.parse(rec[fname]);
        }
    }

    _format_base64(fname,rec,fdesc,locale,forceEnumLabel=false) 
    {
        if(rec[fname])
        {
            rec[fname] = Buffer.from(rec[fname], 'base64').toString('utf8');
        }
    }
    
    /**
     * format enum value as {html,value}
     * use fname and fname__html fields to build value and html.
     * @param {*} fname 
     * @param {*} rec 
     * @param {*} fdesc 
     * @param {*} locale 
     * @param {*} forceEnumLabel 
     * @returns 
     */
    _format_enum(fname,rec,fdesc,locale,forceEnumLabel=false) 
    {
        let v = rec[fname];
        if(v == undefined)
            return;

        if((v || v==='') && typeof (rec[fname+'__html']) != "undefined")
        {
            rec[fname] = {
                value:v,
                html:rec[fname+'__html']
            };
            delete rec[fname+'__html'];
        }
        else
        {
            if(!forceEnumLabel && v && v.value && v.html)
                rec[fname] = v;
            else
            {
                let v2;
                if(typeof v == "object")
                {
                    v2 = v.value;
                }
                else
                    v2 = v;

                let html = v && (fdesc && fdesc.getEnum && fdesc.getEnum(v2,",",locale)) || '';
                rec[fname] = {
                    value:v2,
                    html
                };    
            }
        }
    }

    /**
     * format as enum with email {value: <fname> ,html:<fname>__html, email: <fname>__email}
     * 
     * @param {*} fname 
     * @param {*} rec 
     * @param {*} fdesc 
     * @param {*} locale 
     */
    _format_enum_with_email(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        if(v && typeof (rec[fname+'__html']) != "undefined")
        {
            rec[fname] = {
                value:v,
                html:rec[fname+'__html']
            };
            delete rec[fname+'__html'];

            // add email if exists
            if(typeof (rec[fname+'__email']) != "undefined")
            {
                rec[fname].email = rec[fname+'__email'];
                delete rec[fname+'__email'];
            }
        }
        else
        {
            if(v && v.value && v.html)
                rec[fname] = v;
            else
            {
                let html = v && (fdesc && fdesc.getEnum && fdesc.getEnum(v,",",locale)) || '';
                rec[fname] = {
                    value:v,
                    html
                };    
            }
        }
    }    

    /**
     * format as enum with a class {value: <fname> ,html:<fname>__html, cls: <fname>__cls}
     * 
     * @param {*} fname 
     * @param {*} rec 
     * @param {*} fdesc 
     * @param {*} locale 
     */
    _format_enum_with_class(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        if(v && typeof (rec[fname+'__html']) != "undefined")
        {
            rec[fname] = {
                value:v,
                html:rec[fname+'__html']
            };
            delete rec[fname+'__html'];

            // add class if exists
            if(typeof (rec[fname+'__cls']) != "undefined")
            {
                rec[fname].cls = rec[fname+'__cls'];
                delete rec[fname+'__cls'];
            }
        }
        else
        {
            if(v && v.value && v.html)
                rec[fname] = v;
            else
            {
                let html = v && (fdesc && fdesc.getEnum && fdesc.getEnum(v,",",locale)) || '';
                rec[fname] = {
                    value:v,
                    html,
                    cls:''
                };    
            }
        }
    }        

    _format_enum_reg(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        let html = v || "";

        if(html)
        {
            let format = fdesc._prop("x-enum-reg-format");
            if(format)
            {
                let pattern = format.reg;
                let regEx = new RegExp(pattern, "gm");
                let rep = format.html;
                html = v.replace(regEx,rep) || v || "";
            }    
        }

        rec[fname] = {
            value:v,
            html
        };
    }

    _format_enum_email_name(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        let html = v || "";

        if(html)
        {
            try {
                html = html
                .split("@")[0]
                .split(".")
                .map(mot => mot.charAt(0).toUpperCase() + mot.slice(1).toLowerCase())
                .join(' ');
            }
            catch(error)
            {
            }
        }

        rec[fname] = {
            value:v,
            html
        };
    }    

    _format_enum_multi_fields(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        
        let value,html;
        if(typeof v == "object")
        {
            value = v.value || "";
            html = v.html || "";
        }

        if(value && html)
        {
            try 
            {
                let fields = {};
                let values = value.split("|");
                let html2 = html.split(/,[ ]?/);
                let i = 0;
                for (let i=0;i< values.length;i++) 
                {
                    let v2 = values[i];
                    if(!v2)
                        continue;

                    let v3 = v2.split("@");
                    if(v3.length != 2)
                        continue;

                    let afname2 = v3[1].split('.');
                    let fname2 = afname2[0];
                    let propDef = afname2.length == 2 ? afname2[1] : "properties";
                    if(!fields[fname2])
                        fields[fname2] = {value:[],html:[],propDef};

                    fields[fname2].value.push(v3[0]);
                    //fields[fname2].html.push(html2[i] || "");
                    fields[fname2].html.push(v3[0] || "");
                }

                delete rec[fname];

                for (let fname3 in fields)
                {
                    let field = fields[fname3];
                    let v4 = "|"+field.value.join("|")+"|";
                    let hmtl4 = field.html.join(",");
                    rec[fname3] = {value:v4,html:hmtl4,propDef:field.propDef||null};
                }
            }
            catch(error)
            {
            }
        }
        else {
            rec[fname] = {
                value:'',
                html:''
            };             
        }
    } 

    _format_enum_upper_initial_html(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        if(v.html)
            v.html = v.html
                .split(' ')
                .map(mot => mot.charAt(0).toUpperCase() + mot.slice(1).toLowerCase())
                .join(' ');
    }    

    _format_enum_static(fname,rec,fdesc,locale) 
    {
        if(rec[fname])
        {
            let v = rec[fname];
            let html;
            if(fdesc && fdesc.getEnum)
                html = fdesc.getEnum(v,",",locale);

            rec[fname] = {
                value:v,
                //html:(locale && locale.e_(rec[fname],fname)) || rec[fname]
                html
            };
        }
    }

    _formatLocaleValues(fname,rec) 
    {
        rec[fname] = this._locale.v_(rec[fname]);
    }    

    _parseValue(v,field) {
        if(v && typeof v.value !="undefined")
            v = v.value;

        if(v === null || typeof v == "undefined")
            return "NULL";

        const type = field.type();
        if(type == 'string')
        {
            if(typeof v == "object")
                return "''";

            if(v.replace)
                return "'"+v
                    .replace(/'/g,"\\'")
                    .replace("||","")
                    +"'";
            else
                return v
        }
        
        if(type == 'integer')
            return 0+v;

        if(type == 'date' || type == 'timestamp')
        {
            if(typeof v == "integer")
                return v;

            if(v.includes && v.includes("NOW"))
                return v.replace(/now(\s*[(]\s*[)])?/i,'NOW()');

            if(v === '' || v == '-')
                return "NULL";

            return "'"+v+"'";
        }

        return "'"+v+"'";
    }

    _fieldDef(schemaField) {
        const fname = schemaField.dbName();
        let def = fname+" ";
        const type = schemaField.type().toLowerCase();
        const size = schemaField._prop("maxLength",null);
        let defaultV = schemaField._prop("default",null);
        let isPrimaryKey = false;
        let nullable = schemaField._prop("nullable",null)

        switch(type) {
            case 'string':
                if(size)
                    def += "VARCHAR("+size+")";
                else
                def += "TEXT";

                if(defaultV!==null)
                    def +=  " DEFAULT '"+defaultV+"'";
        
                break;
            case 'integer':
                def += "INT("+(size||"11")+")";
                if(schemaField._prop("x-auto-id",null))
                {
                    def += " AUTO_INCREMENT NOT NULL";
                    isPrimaryKey = true;
                    nullable = null;
                }
                else if(defaultV!==null)
                    def +=  " DEFAULT "+defaultV;

                break;
            case 'date':
                def += "DATE";
                if(defaultV!==null)
                    def +=  " DEFAULT "+defaultV;

                // date nullable by default
                nullable = schemaField._prop("nullable",true)                    
                break;                    
            case 'float':
            case 'number':
                def += "FLOAT";
                if(defaultV!==null)
                    def +=  " DEFAULT "+defaultV;
            break;
            case 'double':
                def += "DOUBLE";
                if(defaultV!==null)
                    def +=  " DEFAULT "+defaultV;
                break;
            default:
                throw new Error("unknown field type "+type);
        }

        if(nullable!==null)
            def += nullable ? ' NULL' : " NOT NULL";

        let key = '';
        if(isPrimaryKey)
            key = "PRIMARY KEY("+fname+")";       

        return {def,key};
    }    

    _mapFieldsTypes(fields) {
        let fdefs = [];
        let fkeys = [];
        for(let p in fields)
        {
            const {def,key} = this._fieldDef(fields[p]);
            fdefs.push(def);
            if(key)
                fkeys.push(key);
        }
        return {fdefs,fkeys};
    }

    async _fixMissingField(error,view) 
    {
        if(!view)
            return null;
        
        const model = view.model();
        const {colName,dbPrefix} = this._collection(model);
        
        const fPrefix = view.fieldPrefix();

        const matches = error.message.match(/Unknown column '([^']+)' in 'field list'/);
        if(matches)
        {
            // get field schema 
            let fname = matches[1];
            if(fPrefix)
                fname = fname.replace(new RegExp("^"+fPrefix),'');  

            const schema = model.schema();
            let fieldSchema = schema.field(fname);
            if(!fieldSchema)
            {
                const dftPrefix = schema.fieldPrefix()
                
                if(dftPrefix)
                    fname = fname.replace(new RegExp("^"+dftPrefix),'');  
                
                fieldSchema = schema.field(fname);
            }

            if(fieldSchema)
            {
                // get field sql definition
                const {def,key} = this._fieldDef(fieldSchema);
                if(def)
                {
                    // create ALTER query
                    let qs = this._buildQuery(
                        view, 
                        'add_field',
                        "ALTER TABLE %table% ADD COLUMN %field_def%",
                            {
                                table: dbPrefix+colName,
                                field_def: def
                            }
                    );

                    // exec ALTER query
                    if(this.config.log)
                        debug.log(qs+ " / add field"+fname);

                    const res = await this.query(qs,view);
                    if(res)
                        return true;
                }
            }

            throw new Error("cant fix SQL query or ALTER add field "+fname+" to table "+dbPrefix+colName+
                " related to view "+view.name()+" in model "+model.name());
        }

        // something went wrong
        return false;
    }

    async _fixMissingTable(error,view) 
    {
        if(!view)
            return null;

        let model = view.model();
        let {colName,dbPrefix,dbName} = this._collection(model);
        let table = colName;

        const matches = error.message.match(/Table '([^.]+).([^']+)' doesn't exist/);
        if(matches)
        {
            // get field schema 
            let dbNname2 = matches[1];
            let missingTable = matches[2];
            if(missingTable != colName && (dbPrefix ? dbName == dbNname2 : true))
            {                
                model = model.modelManager().getModelByCollection(missingTable);
                if(model)
                {
                    table = model.schema().collection();
                    model = model.instance();
                }
                view = null;
            }

            if(missingTable == table)
            {
                // missng table is the current view schema
                const res = await this.createCollection(null,model,view);
                if(res)
                {
                    debug.log("Table created with success "+missingTable);
                }

                return res;
            }
            else
                // something went wrong
                throw new Error("cant fix SQL query or add missing table "+dbNname2+"."+missingTable);
        }

        // something went wrong
        return false;
    }

    /**
     * 
     * @param {*} model 
     * @returns {{colName,dbPrefix,dbName}}
     */
    _collection(model) 
    {
        let col = model.collection() || this.config.table || this.config.collection;

        let {colName,dbName, dbPrefix} = this._getCollectionName(col,model);
        debug.log("MySQL table name: "+ dbPrefix+colName);

        return {colName,dbPrefix,dbName};
    }

   /** 
     *  get collection name, dbName and dbPrefix, by using standard collection from model 
     *  + adding database based on clientId if any. Otherwise dbPrefix == ""
     * 
     * Uses a cache so that the mapping names are only processed once (first use)
     * 
     *  Details :
     *   If a client_id is provided in the model, check the client configuration to get database.
     * 
     *   The db needs a "apply_client_id = "database_prefix", "database_suufix" for the client id to be applied.
     *   ex. ged_<client_id> (database suffix), or <client_id>_ged (database prefix)
     * 
     * @param {DbModelInstance} model db model
     * 
     * @returns { {colName:string,dbName:string,dbPrefix:string} }
     */
   _getCollectionName(colName,model) 
   {
       if(!this._collectionsMap[colName])
       {
            const clientId = model.getClientId();
            let dbName = null;
            let dbPrefix = null      
            if(clientId && this.clientManager)
            {
                let cltInfos = this.clientManager.getClientInfosById(clientId,"mysql");

                if(cltInfos && cltInfos.database)
                {
                     dbName = cltInfos.database;
                     
                     if(cltInfos.apply_client_id == "database_suffix")
                         dbName += clientId;
                     else if(cltInfos.apply_client_id == "database_prefix")
                         dbName = dbName+clientId;
     
                     if(dbName)
                        dbPrefix = dbName+"."
                }
            }
            else
            {
                dbPrefix = "";
            }
     
            this._collectionsMap[colName] = {colName,dbName,dbPrefix};
       }
       
       return this._collectionsMap[colName];
   }    

    /* ============ SUPPORT INTERFACE UNIFIEE BASEE SUR MONGODB ================= */

    /**
     * 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * @param {string} view 
     * @returns 
     */
    async createCollection(options,model,view=null) 
    {
        if(!view)
            view = model ? model.getView(options && (options.view||options.$view)||"record") : null;

        const {colName,dbPrefix} = this._collection(model);
        const fields = model.schema().fields();

        const {fdefs,fkeys} = this._mapFieldsTypes(fields);
        const fields_def = fdefs.join(',');       
        let fields_keys = fkeys.join(',');
        if(fields_keys)
            fields_keys = ','+fields_keys;

        // compile query
        let qs = this._buildQuery(
            view, 
            'create_collection',
            "CREATE TABLE IF NOT EXISTS %table% (%fields_def%%fields_keys%)",
                {
                    table : dbPrefix+colName,
                    db_:dbPrefix,
                    fields_def,
                    fields_keys
                }
        );               

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
        return true;
    }

    /**
     * get an empty record
     * 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * @returns {TMySQLResult}
     */
    getEmpty(options,model) 
    {
        const view = model ? model.getView(options.view||options.$view) : null;
        
        const variables = options.variables || {};

        let data={};
        objectSce.forEachSync(view.fields(),(f,n) => {
            let dft = f.default();
            let type = f.type();

            if(dft && dft.split &&  dft[0] == "%")
            {
                dft = mapper.mapAttribute(dft,variables);
            }

            data[n] = dft ||
                ((type=='string') ? '' :
                (type=='integer') ? 0  : '');
        });

        data = this._formatRecord(data,view);
        let ret = {data};

        if(options.withMeta)
            ret.metadata = view.metadata();

        if(options.withLocale)
            ret.locale = view.locale();

        return ret;
    }

    /**
     * 
     * @param {Record<string,any>} query 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * 
     * @returns {Promise<TMySQLResult>}
     */
    async findOne(query,options,model) 
    {
        const view = model ? model.getView(options.view||options.$view||"record") : null;
        // const col = options.collection || model.collection() || this.config.table || this.config.collection;
        const {dbPrefix,colName} = this._collection(model);

        const where = this._mapWhere(query,view);

        const limit = options.limit||1;
        const skip = options.skip||0;
        const qlimit = this._mapLimit(limit,skip);

        // compile query
        let qs = this._buildQuery(
            view, 
            'findOne',
            "select %fields% from %table% %where% %limit%",
                {
                    table : dbPrefix+colName,
                    db_:dbPrefix,
                    fields: view.fieldsNames(true) || '',
                    where: where,
                    where_and: (where ? where+" AND " : "WHERE "),
                    WHERE:(where ? where : "WHERE 1=1"),
                    limit: qlimit,
                    ...query
                }
        );

        if(this.config.log)
            debug.log("REQ "+qs+ " / $view="+view.name());

        const docs = await this.query(qs,view);
    
        if(docs.length==0)
            return null;

        let data = this._formatRecord(docs[0],view);
        let ret = {data};

        if(options.withMeta)
            ret.metadata = view.metadata();
            
        if(options.withLocale)
            ret.locale = view.locale();
            
        return ret;
    }

    /**
     * 
     * @param {Record<string,any>} query 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * @returns 
     */
    async find(query,options,model) 
    {
        const view = model ? model.getView(options.view||options.$view) : null;
        const {dbPrefix,colName} = this._collection(model);

        const where = this._mapWhere(query,view,true);

        let qlimit,skip,limit;
        if(options.limit)
        {
            limit = options.limit;
            skip = options.skip||0;
            qlimit = this._mapLimit(limit,skip);    
        }
        else
            qlimit='';

        let qs = this._buildQuery(
            view, 
            'find',
            "%select% %fields% from %TABLE% %where% %limit%",
                {
                    table : dbPrefix+colName,
                    db_:dbPrefix,
                    TABLE: dbPrefix+colName+' '+view.tableAlias(),
                    fields: view.fieldsNames(true) || '',
                    where: where,
                    where_and: (where ? where+" AND " : "WHERE "),
                    WHERE:(where ? where : "WHERE 1=1"),
                    limit: qlimit,
                    select: "select SQL_CALC_FOUND_ROWS",
                    ...query
                }
        );

        let nbdocs,data;
        try 
        {
            data = await this.query(qs,view,null,false,
                async (res,con) =>
                {
                    // reuse same con for getting the nb of selected rows that is kept in connection object
                    nbdocs = await con.query("SELECT FOUND_ROWS() as nbrecords",null,null,false);
                });
                
            if(this.config.log)
                debug.log(qs + " -> nb = " + data.length+ " / $view="+view.name());    
        }
        catch(error) 
        {
            debug.error(error);
        }

        const nb = nbdocs?.length && nbdocs[0].nbrecords;
        let pages=null;
        if(nb) {
            pages = {
                offset:skip,
                limit:limit,
                total:nb
            };
        }

        // remap fields?
        if(view.getFieldsFormats()) {
            data = data.map(rec => this._formatRecord(rec,view) );
        }

        let ret = {data,pages};

        if(options.withMeta)
            ret.metadata = view.metadata();

        if(options.withLocale)
            ret.locale = view.locale();

        return ret;
    }

    /**
     * 
     * @param {Record<string,any>} query 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * 
     * @returns 
     */
    async count(query,options,model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        // const col = options.collection || model.collection() || this.config.table|| this.config.collection;
        const {colName,dbName} = this._collection(model);

        const where = this._mapWhere(query,view,true);

        const limit = options.limit||-1;
        const skip = options.skip||0;
        const qlimit = this._mapLimit(limit,skip);
        let qs = this._buildQuery(
            view, 
            'count',
            "count * from %table% %where% %limit%",
                {
                    table : col,
                    where_and: (where ? where+" AND " : "WHERE "),
                    WHERE:(where ? where : "WHERE 1=1"),
                    where: where,
                    limit: qlimit,
                    ...query
                }
        );

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const docs = await this.query(qs,view);

        return docs;
    }

    /**
     * create new record and returns the id of the newly created record
     * 
     * @param {TMySQLRecord} doc 
     * @param {*} options 
     * @param {DbModelInstance} model 
     * 
     * @returns {Promise<any>}
     */
    async insertOne(doc,options,model) 
    {

        const view = model ? model.getView(options.view||options.$view||"record") : this.config;
        // const col = options.collection || model.collection() || this.config.table|| this.config.collection;
        const {colName,dbName} = this._collection(model);

        const variables = options.variables || {};

        let fields = view.fields();
        let values = [];
        let fnames = [];
        objectSce.forEachSync(fields,(field,name)=> 
        {
            fnames.push(name);
            const alias = field.alias();
            let v;

            if(field._prop('x-auto-id',false)===true)
            {
                // do not set autoincrement
                v = "NULL";
            }
            else if(typeof doc[name] != "undefined")
            {
                v = this._parseValue(doc[name],field);
            }
            else if(typeof doc[alias] != "undefined")
            {
                v = this._parseValue(doc[alias],field);
            }
            else
            {
                let dft = field.default();

                if(dft && dft.split &&  dft[0] == "%")
                {
                    dft = mapper.mapAttribute(dft,variables);
                }

                v = this._parseValue(dft,field);    
            }

            values.push(v);
        });

        let qs = this._buildQuery(
            view, 
            'insertOne',
            "INSERT INTO %table% (%fields%) VALUES %values%",
                {
                    table : colName,
                    fields: view.fieldsNamesInsert(true),
                    values: "("+values.join(",")+")",
                    ...doc
                }
        );        

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res= await this.query(qs,view);
        const insertId = res.insertId;
    
        return insertId;
    }

    /**
     * create new records
     * 
     * @param {TMySQLRecord[]} docs
     * @param {TMySQLFindOptions} options
     * @param {DbModelInstance} model 
     * 
     * @returns {Promise<any>}
     */
    async insertMany(docs,options,model) 
    {
        const view = model ? model.getView(options.view||options.$view||"record") : this.config;
        const {colName,dbName} = this._collection(model);

        if(docs.length == 0)
            return null;
    
        const doc = docs[0];
        let fields = view.fields();
        let fnames = [];
        objectSce.forEachSync(fields,(field,name)=> {
            fnames.push(name);
        });
    
        let aValues = [];
        docs.forEach(doc => 
        {
            let values = [];
            objectSce.forEachSync(fields,(field,name)=> 
            {
                let v;
                if(typeof doc[name] == "undefined")
                    v = this._parseValue(field.default(),field);
                else
                    v = this._parseValue(doc[name],field);    
                values.push(v);
            });
    
            aValues.push("("+values.join(",")+")");
        });

        let qs = this._buildQuery(
            view, 
            'insertMany',
            "INSERT INTO %table% (%fields%) VALUES %values%",
                {
                    table : colName,
                    fields: view.fieldsNamesInsert(true),
                    values: aValues.join(","),
                }
        );        
  
        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());
    
        const res= await this.query(qs,view);
        return res;
    }

    /**
     * update a record
     * 
     * @param {TMySQLRecord} doc 
     * @param {boolean} addIfMissing 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * 
     * @returns {Promise<any>}
     */
    async updateOne(query,doc,addIfMissing=false,options,model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        // const col = options.collection || model.collection() || this.config.table|| this.config.collection;
        const {dbPrefix,colName} = this._collection(model);

        let fields = view.fields();
        
        let values = [];
        let fnames = [];
        objectSce.forEachSync(fields,(field,name)=> 
        {
            fnames.push(name);

            let v;
            if(typeof doc[name] == "undefined")
                v = this._parseValue(field.default(),field);
            else
                v = this._parseValue(doc[name],field);

            values.push(v);
        });

        let qlimit,skip,limit;
        if(options.limit)
        {
            limit = options.limit;
            skip = options.skip||0;
            qlimit = this._mapLimit(limit,skip);    
        }
        else
            qlimit = 'LIMIT 1';    

        const where = this._mapWhere(query,view,false); // where without table prefix (_oid but not T1._oid)
        let qs;
        if(addIfMissing)
        {
            let op = "REPLACE ";

            qs = this._buildQuery(
                view, 
                'replaceOne',
                "%replace% INTO %table% (%fields%) VALUES (%values%)",
                    {
                        op:op,
                        update:op,
                        replace:op,

                        table : dbPrefix+colName,
                        TABLE: dbPrefix+colName, // +' '+view.tableAlias(),
                        db_:dbPrefix,

                        fields:view.fieldsNamesUpdate(true).join(','),
                        values:values.join(','),
    
                        where: '',
                        WHERE:'',
                        limit: '',
                        ...query,
                        ...doc
                    }
            );             
        }
        else
        {
            let op = "UPDATE ";

            // define fname=value csv list
            let aFnames = view.fieldsNamesUpdate(true);
            let fields_values = aFnames.map((name,i)=>name+'='+values[i]).join(',');
            qs = this._buildQuery(
                view, 
                'updateOne',
                "%update% %table% SET %fields_values% %where% %limit%",
                    {
                        op:op,
                        update:op,

                        table : dbPrefix+colName,
                        TABLE: dbPrefix+colName, // +' '+view.tableAlias(),
                        db_:dbPrefix,

                        fields: view.fieldsNamesUpdate(true),
                        fields_values:fields_values,
    
                        where: where,
                        where_and: (where ? where+" AND " : "WHERE "),
                        WHERE:(where ? where : "WHERE 1=1"),

                        limit: qlimit,
                        ...query,
                        ...doc
                    }
            );         
        }       

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
    
        return res;
    }

    /**
     * @param {*} query 
     * @param {TMySQLRecord[]} docs 
     * @param {boolean} addIfMissing 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * 
     * @returns {Promise<any>}
     */
    async updateMany(query,docs,addIfMissing=true,options, model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        // const col = options.collection || model.collection() || this.config.table|| this.config.collection;
        let {dbPrefix,colName} = this._collection(model);

        if(docs.length == 0)
            return null;

        const where = this._mapWhere(query,view);

        colName = colName || this.config.table;

        let qs = this.queries?.updateMany || 
            (addIfMissing ? "REPLACE " : "UPDATE ") +dbPrefix.colName+" SET ";
    
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

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
    
        return res;
    }

    /**
     * @param {*} query 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * 
     * @returns {Promise<any>}
     */
    async deleteOne(query,options, model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        let {dbPrefix,colName} = this._collection(model);

        let qlimit,skip,limit;
        if(options.limit)
        {
            limit = options.limit;
            skip = options.skip||0;
            qlimit = this._mapLimit(limit,skip);    
        }
        else
            qlimit = 'LIMIT 1';    
        
        const where = this._mapWhere(query,view,false);

        let qs = this._buildQuery(
            view, 
            'deleteOne',
            "DELETE FROM %TABLE% %where% %limit%",
                {
                    table : dbPrefix+colName,
                    TABLE: dbPrefix+colName,
                    db_:dbPrefix,

                    where,
                    where_and: (where ? where+" AND " : "WHERE "),
                    WHERE:(where ? where : "WHERE 1=1"),
                    limit:qlimit,
                    ...query
                }
        );

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
        const deleteRows = res.affectedRows||0;
        debug.log("deleted rows = "+deleteRows);
    
        return deleteRows;
    }

    /**
     * @param {*} query 
     * @param {TMySQLFindOptions} options 
     * @param {DbModelInstance} model 
     * 
     * @returns {Promise<any>}
     */
    async deleteMany(query,options, model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        let {dbPrefix,colName} = this._collection(model);

        let qs = this.queries?.deleteMany || "DELETE FROM "+dbPrefix+colName;
        
        const where = this._mapWhere(query,view);
        qs += where;

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
    
        return res;
    }
}

class MySqlSce
{
  constructor() {
        this.instances={};
  }
    getInstance(instName) {
        if(this.instances[instName])
            return this.instances[instName];

        return (this.instances[instName] = new MySqlInstance());
  }
}

module.exports = new MySqlSce();

module.exports.MySqlHandlerBase = MySqlHandlerBase;
module.exports.MySqlInstance = MySqlInstance;
module.exports.MySqlSce = MySqlSce;