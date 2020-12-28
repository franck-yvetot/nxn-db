const debug = require("@nxn/debug")('MYSQL_SCE');
const {configSce} = require('@nxn/boot');
const {objectSce} = require("@nxn/ext");

const mysql = require('mysql2/promise');

class MySqlInstance
{
    constructor(config) 
    {
        if(config)
    this.init(config);
}

    async init(config) 
    {
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

    async query(q,view=null,values=null,reconnect=false) 
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
            debug.error("ERROR message "+error.message);

            if(error.message == "Can't add new command when connection is in closed state" || error.code == 'EPIPE')
                {
                    debug.error("try reconnecting...");
                return this.query(q,view,values,true);
            }
            if(error.code == 'ER_BAD_FIELD_ERROR')
            {
                debug.error("try adding new field...");
                const isOk = await this._fixMissingField(error,view);
                if(isOk)
                    return this.query(q,view,values,false);
            }
            if(error.code == 'ER_NO_SUCH_TABLE')
            {
                debug.error("try adding new table...");
                const isOk = await this._fixMissingTable(error,view);
                if(isOk)
                    return this.query(q,view,values,false);
                }
                throw error;            
            }
    
    if(res.insertId)
      return res.insertId;

    const [rows, fields] = res;     
    return rows;
  }

  /* ============ PRIVATE METHODS ================= */

    _mapWhere(query,view,withTablePrefix=true) {
    var where = "";

        const schema = view.schema();

    if(query)
        {
            const prefix = schema.fieldPrefix();

            let aWhere = [];
        objectSce.forEachSync(query, (value,name)=> {
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
                if(defaultV!==null)
                    def +=  " DEFAULT "+defaultV;

                break;
            case 'date':
                def += "DATE";
                if(defaultV!==null)
                    def +=  " DEFAULT "+defaultV;
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

    async _fixMissingField(error,view) {
        const model = view.model();
        const table = this._collection(model);
        const fPrefix = view.fieldPrefix();

        const matches = error.message.match(/Unknown column '([^']+)' in 'field list'/);
        if(matches)
        {
            // get field schema 
            let fname = matches[1];
            if(fPrefix)
                fname = fname.replace(new RegExp("^"+fPrefix),'');  

            const fieldSchema = model.schema().field(fname);

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
                                table,
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
            throw new Error("cant fix SQL query or ALTER add field "+fname+" to table "+table);
            // something went wrong
            return false;
        }

        // something went wrong
        return false;
    }

    async _fixMissingTable(error,view) {
        const model = view.model();
        const table = this._collection(model);

        const matches = error.message.match(/Table '([^.]+).([^']+)' doesn't exist/);
        if(matches)
        {
            // get field schema 
            let dbNname = matches[1];
            let missingTable = matches[2];
            if(missingTable == table)
            {
                // missng table is the current view schema
                const res = this.createCollection(null,model,view);
            }
            else
                throw new Error("cant fix SQL query or add missing table "+dbNname+"."+missingTable);

            // something went wrong
            return false;
        }

        // something went wrong
        return false;
    }

    _collection(model) {
        return model.collection() || this.config.table || this.config.collection;
    }

    /* ============ SUPPORT INTERFACE UNIFIEE BASEE SUR MONGODB ================= */

    async createCollection(options,model,view=null) 
    {
        if(!view)
            view = model ? model.getView(options.view||options.$view||"record") : null;
        const col = this._collection(model);
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
                    table : col,
                    fields_def: fields_def
                }
        );               

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
        return true;
    }

    async findOne(query,options,model) 
    {
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

        const docs = await this.query(qs,view);
    
        if(docs.length==0)
    return null;

        let data = this._formatRecord(docs[0],view);
        let ret = {data};

        if(options.withMeta)
            ret.metadata = view.metadata();
            
        return ret;
    }

    getEmpty(options,model) 
    {
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

    async find(query,options,model) 
    {
        const view = model ? model.getView(options.view||options.$view) : null;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

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
                    table : col,
                    TABLE: col+' '+view.tableAlias(),
                    fields: view.fieldsNames(true) || '',
                    where: where,
                    WHERE:(where ? where : "WHERE 1=1"),
                    limit: qlimit,
                    select: "select SQL_CALC_FOUND_ROWS",
                    ...query
                }
        );

        let data = await this.query(qs,view);
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

    async count(query,options,model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

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

    async insertOne(doc,options,model) 
    {

        const view = model ? model.getView(options.view||options.$view||"record") : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

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

        const res= await this.query(qs,view);
        const insertId = res.insertId;
    
    return insertId;
}

    async insertMany(docs,options,model) 
    {
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
        docs.forEach(doc => 
        {
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
    
        const res= await this.query(qs,view);
        return res;
}

    async updateOne(query,doc,addIfMissing=false,options,model) {

        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

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
                        table : col,
                        TABLE: col, // +' '+view.tableAlias(),
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
                        table : col,
                        TABLE: col, // +' '+view.tableAlias(),
                        fields: view.fieldsNamesUpdate(true),
                        fields_values:fields_values,
    
                        where: where,
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

    async updateMany(query,docs,addIfMissing=true,options, model) 
    {
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

        const res = await this.query(qs,view);
    
    return res;
}

    async deleteOne(query,options, model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

    let qs = this.queries.deleteOne || "DELETE FROM "+col;
        
        const where = this._mapWhere(query,view);
    qs += where;

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
    
    return res;
}

    async deleteMany(query,options, model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;
        const col = options.collection || model.collection() || this.config.table|| this.config.collection;

    let qs = this.queries.deleteMany || "DELETE FROM "+col;
        
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