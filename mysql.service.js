const debug = require("@nxn/debug")('MYSQL_SCE');
const {configSce, FlowNode} = require('@nxn/boot');
const {objectSce} = require("@nxn/ext");

const mysql = require('mysql2/promise');

let pools = {};
class MySqlPool
{
    constructor(conInfo) {
        this.conInfo = conInfo;
    }

    async createPool() 
    {
        const host = conInfo.MYSQL_HOST;
        const port   = conInfo.MYSQL_PORT || 3306;
        const password = conInfo.MYSQL_PWD;
        const database = conInfo.MYSQL_DB;
        const user = conInfo.MYSQL_USER;

        this.pool = mysql.createPool(
        {
            connectionLimit: 100,
            host,
            user,
            password,
            database,
            debug: false
        });
    }

    async getCon() 
    {
        let con = await pool.getConnection();        
        return con;
    }

    async releaseCon(con) {
        con.release();
    }

    static async getCon(conInfo,cb) 
    {
        let poolId = Object.values(conInfo).join('_');
        
        if(!pools[poolId])
            pools[poolId] = new MySqlPool(conInfo);

        let pool = pools[poolId];

        return pool.getCon();
    }
}

class MySqlInstance extends FlowNode
{
    constructor(inst) {
        super(inst);
    } 

    async init(config,ctxt,...injections)
    {
        if(!config || this.config)
            return;

        super.init(config,ctxt,injections); 

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
        }   
        catch(err) {
            throw err;
        }

        return this.conInfo;
    }

    async connect(force=false) 
    {
        if(!force && this.connected)
            return this.con; 

        try 
        {            
            let conInfo = await this.loadConInfo();

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

            // for cloud run
            if(conInfo.MYSQL_SOCKET_PATH)
                // e.g. '/cloudsql/project:region:instance'
                // ex. '/cloudsql/presence-talents:europe-west1:presence-talents-preprod56'
                params.socketPath = conInfo.MYSQL_SOCKET_PATH
            else
            {
                if(!conInfo.MYSQL_HOST)
                    throw "cant find MYSQL_HOST for connecting MySql instance";
                
                params.host = conInfo.MYSQL_HOST;
                params.port = conInfo.MYSQL_PORT || 3306;
            }

            if(conInfo.MYSQL_TIMEOUT) 
            {
                params.connectTimeout = conInfo.MYSQL_TIMEOUT;
                // params.waitForConnections = true;
                
            }

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
                debug.log("MYSQL CONNECTED TO DB: "+database);
            else
                throw "MYSQL CANT CONNECT TO DB :"+database;

            this.connected = true;
            return this.con;
        }
        catch(err) {
            debug.error(`cant connect to MySql instance `+err);
            return Promise.reject({error:500,error:"cant conect to MySql "+err});
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

    async releaseCon(con) 
    {
        // for pool
    }

    async query(q,view=null,values=null,reconnect=false) 
    {
        let con = await this.connect(reconnect);

        let res;
        
        try 
        {
            if(values)
                res = await con.execute(q,values);
            else
                res = await con.query(q);

            this.releaseCon(con);
        } 
        catch (error) 
        {
            this.releaseCon(con);

            debug.error("ERROR in MYSQL query "+q);
            debug.error("ERROR message "+error.message);

            if(error.message == "Can't add new command when connection is in closed state" || error.code == 'EPIPE')
            {
                debug.error("try reconnecting...");
                return this.query(q,view,values,true);
            }
            if(error.code == 'ECONNABORTED')
            {
                debug.error("MYSQL Connection error, reconnecting...");
                return this.query(q,view,values,false);
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

    _mapWhere(query,view,withTablePrefix=true) 
    {
        var where = "";

        const schema = view.schema();

        if(query)
        {
            const prefix = schema.fieldPrefix();

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

    _mapLimit(limit=0,skip=0) 
    {
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
        const schema = view.schema();

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
                const fdesc = schema.field(k);
                if(typeof this[func] == "function")
                    this[func](k,rec,fdesc,locale);
            }
        });

        return rec;
    }

    _format_json(fname,rec,fdesc,locale) 
    {
        if(rec[fname])
        {
            rec[fname] = JSON.parse(rec[fname]);
        }
    }

    _format_base64(fname,rec,fdesc,locale) 
    {
        if(rec[fname])
        {
            rec[fname] = Buffer.from(rec[fname], 'base64').toString('utf8');
        }
    }
    
    _format_enum(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        if(v && typeof (rec[fname+'__html']) != "undefined")
        {
            rec[fname] = {
                value:v,
                html:rec[fname+'__html']
            };
            delete rec[fname+'__html'];
        }
        else
        {
            let html = v && (fdesc.getEnum && fdesc.getEnum(v,",",locale)) || '';
            rec[fname] = {
                value:v,
                html
            };
        }
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
            throw new Error("cant fix SQL query or ALTER add field "+fname+" to table "+table+
                " related to view "+view.name()+" in model "+model.name());
            // something went wrong
            return false;
        }

        // something went wrong
        return false;
    }

    async _fixMissingTable(error,view) {
        let model = view.model();
        let table = this._collection(model);

        const matches = error.message.match(/Table '([^.]+).([^']+)' doesn't exist/);
        if(matches)
        {
            // get field schema 
            let dbNname = matches[1];
            let missingTable = matches[2];
            if(missingTable != table)
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
                const res = this.createCollection(null,model,view);
                if(res)
                {
                    debug.log("Table created with success "+missingTable);
                }

                return res;
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
            view = model ? model.getView(options && (options.view||options.$view)||"record") : null;
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
                    fields_def,
                    fields_keys
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
            
        if(options.withLocale)
            ret.locale = view.locale();
            
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

        if(options.withLocale)
            ret.locale = view.locale();

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

        if(options.withLocale)
            ret.locale = view.locale();

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
                    table : col,
                    TABLE: col,

                    where,
                    WHERE:(where ? where : "WHERE 1=1"),
                    limit:qlimit,
                    ...query
                }
        );

        if(this.config.log)
            debug.log(qs+ " / $view="+view.name());

        const res = await this.query(qs,view);
        const deleteRows = res.affectedRows||0;
    
        return deleteRows;
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