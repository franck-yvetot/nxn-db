const debug = require("@nxn/debug")('MYSQL_SCE');
const {configSce} = require('@nxn/boot');
const {objectSce} = require("@nxn/ext");

const mysql = require('mysql2/promise');

class MySqlInstance
{
  constructor(config) {
        if(config)
    this.init(config);
}

async init(config) {
  if(!config || this.config)
    return;

      this.config=config;
  this.conPath = config.conPath || '.MySql';
  }

    async connect(force=false) 
    {
        if(!force && this.connected)
        return true; 

    // buckets config
        try 
        {            
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

    async close() 
    {
  if(this.connected)
  {
    this.connected = false;
    const con = this.con;
    await con.end();
  }
}

    async query(q,values,reconnect=false) 
    {
        await this.connect(reconnect);

    let res;
        
        try {
    if(values)
      res = await this.con.execute(q,values);
    else
      res = await this.con.query(q);
        } 
        catch (error) 
        {
                debug.error("ERROR in MYSQL query "+q);
                if(error.message == "Can't add new command when connection is in closed state" 
            || error.code == 'EPIPE')
            //message == "This socket has been ended by the other party")
                {
                    debug.error("try reconnecting...");
                    return this.query(q,values,true);
                }
                throw error;            
            }
    
    if(res.insertId)
      return res.insertId;

    const [rows, fields] = res;     
    return rows;
  }

  /* ============ SUPPORT INTERFACE UNIFIEE BASEE SUR MONGODB ================= */
    _mapWhere(query,view) {
    var where = "";

        const schema = view.schema();

    if(query)
        {
            const prefix = schema.fieldPrefix();

            let aWhere = [];
        objectSce.forEachSync(query, (value,name)=> {
                const fw = view.getFieldWhere(name,value);
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

    _mapLimit(limit=0,skip=0) {
    let s='';
    if(limit) 
    {
            s = " LIMIT "+skip+","+limit;
    }

    return s;
}

    _buildQuery(view, viewName,defaultQuery,map) {
        let qs = (view && view.getQuery(viewName)) || (this.config.queries && this.config.queries[viewName]) 
        || defaultQuery;

        const qs2 = qs.replace(/%([a-zA-Z_][a-zA-Z0-9_]+)%/g, (match,p1) => { 
            return map[p1];
        })
        
        return qs2;
    }

    _formatRecord(rec,view) {
        const format = view.getFieldsFormats();
        const locale = view.locale();

        if(!format)
            return  Object.assign({}, rec);
        
        objectSce.forEachSync(format,(vcsv,k) => 
        {
            let av = vcsv.split(',');
            for (let i=av.length-1;i>=0;i--)
            {
                // execute in reverse order
                let v = av[i].trim();
                const func = "_format_"+v;
                if(typeof this[func] == "function")
                    this[func](k,rec,locale);
            }
        });

        return rec;
    }

    _format_json(fname,rec) 
    {
        if(rec[fname])
        {
            rec[fname] = JSON.parse(rec[fname]);
        }
    }

    _format_base64(fname,rec) 
    {
        if(rec[fname])
        {
            rec[fname] = Buffer.from(rec[fname], 'base64').toString('utf8');
        }
    }
    
    _format_enum(fname,rec) 
    {
        if(rec[fname] && typeof (rec[fname+'__html']) != "undefined")
        {
            rec[fname] = {
                value:rec[fname],
                html:rec[fname+'__html']
            };
            delete rec[fname+'__html'];
        }
        else
            return {value:rec[fname],html:''};
    }

    _format_enum_static(fname,rec,locale) 
    {
        if(rec[fname])
        {
            rec[fname] = {
                value:rec[fname],
                html:(locale && locale.e_(rec[fname],fname)) || rec[fname]
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
            if(v.replace)
            return "'"+v.replace(/'/g,"\\'")+"'";
            else
                return v;
        
        if(type == 'integer')
            return 0+v;

        if(type == 'date' || type == 'timestamp')
        {
            if(typeof v == "integer")
                return v;
            if(v.includes && v.includes("NOW"))
                return v.replace(/now(\s*[(]\s*[)])?/i,'NOW()');
            return v;
        }

        return "'"+v+"'";
    }

    _fieldDef(schemaField) {
        const fname = schemaField.dbName();
        let def = fname+" ";
        const type = schemaField.type().toLowerCase();
        const size = schemaField._prop("size",null);
        let isPrimaryKey = false;
        switch(type) {
            case 'string':
                if(size)
                    def += "VARCHAR("+size+")";
                else
                def += "TEXT";
                break;
            case 'integer':
                def += "INT("+(size||"11")+")";
                if(schemaField._prop("x-auto-id",null))
                {
                    def += " AUTO_INCREMENT NOT NULL";
                    isPrimaryKey = true;
                }
                break;
            case 'date':
                def += "DATE";
                break;                    
            case 'float':
                def += "FLOAT";
                break;
            default:
                throw new Error("unknown field type "+type);
        }

        let key = '';
        if(isPrimaryKey)
            key = "PRIMARY KEY("+fname+")";       

        return {def,key};
    }    

    _mapFieldsTypes(metadata) {
        fdefs = [];
        keys = [];
        for(let i = 0 ; i < metadata.length;i++)
        {
            const {def,key} = this._fieldDef(metadata[i]);
            fdefs.push(def);
            if(key)
                fkeys.push(key);
        }
        return {fdefs,fkeys};
    }

    async createCollection(model) {
        const view = model ? model.getView(options.view||options.$view||"record") : null;
        const col = model.collection() || this.config.table || this.config.collection;

        const {fdefs,fkeys} = this._mapFieldsTypes(view.metadata());
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
                    table : col,
                    fields_def: fieldsDef
                }
        );               

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs);
        return true;
    }

    async findOne(query,options,model) {

        const view = model ? model.getView(options.view||options.$view||"record") : null;
        const col = options.collection || model.collection() || this.config.table || this.config.collection;

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
                    table : col,
                    fields: view.fieldsNames(true) || '',
                    where: where,
                    WHERE:(where ? where : "WHERE 1=1"),
                    limit: qlimit,
                    ...query
                }
        );

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const docs = await this.query(qs);
    
        if(docs.length==0)
    return null;

        let data = this._formatRecord(docs[0],view);
        let ret = {data};

        if(options.withMeta)
            ret.metadata = view.metadata();
            
        return ret;
    }

    getEmpty(options,model) {
        const view = model ? model.getView(options.view||options.$view) : null;

        let data={};
        objectSce.forEachSync(view.fields(),(f,n) => {
            data[n] = 
            (f.type=='string') ? '' :
            (f.type=='integer') ? 0  : 
            '';
        });

        data = this._formatRecord(data,view);
        let ret = {data};

        if(options.withMeta)
            ret.metadata = view.metadata();

        return ret;
}

    async find(query,options,model) {
        const view = model ? model.getView(options.view||options.$view) : null;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

        const where = this._mapWhere(query,view);

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
            "%select% %fields% from %table% %where% %limit%",
                {
                    table : col,
                    fields: view.fieldsNames(true) || '',
                    where: where,
                    WHERE:(where ? where : "WHERE 1=1"),
                    limit: qlimit,
                    select: "select SQL_CALC_FOUND_ROWS",
                    ...query
                }
        );

        let data = await this.query(qs);
        if(this.config.log)
            debug.log(qs + " -> nb = " + data.length+ " / $view="+view.name());

        const nbdocs = await this.query("SELECT FOUND_ROWS() as nbrecords");
        const nb = nbdocs[0].nbrecords;
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

        return ret;
}

    async count(query,options,model) {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

        const where = this._mapWhere(query,view);

        const limit = options.limit||-1;
        const skip = options.skip||0;
        const qlimit = this._mapLimit(limit,skip);
        let qs = this._buildQuery(
            view, 
            'count',
            "count * from %table% %where% %limit%",
                {
                    table : col,
                    WHERE:(where ? where : "WHERE 1=1"),
                    where: where,
                    limit: qlimit,
                    ...query
                }
        );

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const docs = await this.query(qs);
    return docs;
}

    async insertOne(doc,options,model) {

        const view = model ? model.getView(options.view||options.$view||"record") : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

        let fields = view.fields();
    let values = [];
        let fnames = [];
        objectSce.forEachSync(fields,(field,name)=> {
        fnames.push(name);

            let v;
            if(typeof doc[name] == "undefined")
                v = this._parseValue(field.default(),field);
            else
                v = this._parseValue(doc[name],field);

            values.push(v);
    });

        let qs = this._buildQuery(
            view, 
            'insertOne',
            "INSERT INTO %table% (%fields%) VALUES %values%",
                {
                    table : col,
                    fields: view.fieldsNamesInsert(true),
                    values: "("+values.join(",")+")",
                    ...doc
                }
        );        

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res= await this.query(qs);
        const insertId = res.insertId;
    
    return insertId;
}

    async insertMany(docs,options,model) {
        const view = model ? model.getView(options.view||options.$view||"record") : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

    if(docs.length == 0)
        return null;
   
    const doc = docs[0];
        let fields = view.fields();
    let fnames = [];
        objectSce.forEachSync(fields,(field,name)=> {
        fnames.push(name);
    });
   
    let aValues = [];
        docs.forEach(doc => {
        let values = [];
            objectSce.forEachSync(fields,(field,name)=> {
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
                    table : col,
                    fields: view.fieldsNamesInsert(true),
                    values: aValues.join(","),
                }
        );        
  
        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());
    
        const res= await this.query(qs);
        return res;
}

    async updateOne(query,addIfMissing=true,options,model) {

        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

    let qs = this.queries.updateOne || 
        (addIfMissing ? "REPLACE " : "UPDATE ")+col+" SET ";

        const where = this._mapWhere(query,view);

    let fnames = [];
    let values = [];
    objectSce.forEachSync(doc,(value,name)=> {
        fnames.push(name);
        values.push(value);
    });

    qs += "("+fnames.join(",")+") "+ "VALUES ('"+values.join("','")+"')";
    qs += where;

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs);
    
    return res;
}

    async updateMany(query,docs,addIfMissing=true,options, model) {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

    if(docs.length == 0)
        return null;

        const where = this._mapWhere(query,view);

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

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs);
    
    return res;
}

    async deleteOne(query,options, model) {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

    let qs = this.queries.deleteOne || "DELETE FROM "+col;
        
        const where = this._mapWhere(query,view);
    qs += where;

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs);
    
    return res;
}

    async deleteMany(query,options, model) {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

    let qs = this.queries.deleteMany || "DELETE FROM "+col;
        
        const where = this._mapWhere(query,view);
    qs += where;

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs);
    
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