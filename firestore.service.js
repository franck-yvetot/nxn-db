const debug = require("@nxn/debug")('FIRESTORE');
const admin = require('firebase-admin');

const Firestore = require('@google-cloud/firestore');
const { getFirestore } = require('firebase-admin/firestore');

const {configSce, FlowNode} = require('@nxn/boot');
const {objectSce} = require("@nxn/ext");

const FieldFormater = require('@nxn/db/field_formater.class');
// const objectService = require("@nxn/ext/object.service");

const formater = new FieldFormater();

/**
 * @config: 
 *     upath: firestore@googleapi
 *     conPath: .firestore
 *     # apply_client_id = coll_prefix | coll_suffix | none | db
 *     apply_client_id: coll_suffix
 *     clientManager: clientManager
 */

class FireStoreInstance extends FlowNode
{
    /**
     * define how to handle "client_id" option
     *  
     * @type { "coll_suffix" | "coll_prefix" | "none" | "db"} */
    apply_client_id;

    /** db by name
     * @type {Record<string,any>}
    */
    dbByName = {};

    /** default db */
    db;

    /** @type {import("../../clients/services/clientManager.service").IClientManager} */
    clientManager; // injection

    constructor(inst) {
        super(inst);
    } 

    async init(config,ctxt,...injections)
    {
        if(!config || this.config)
            return;

        super.init(config,ctxt,injections);

        this.conPath = config.conPath || config.conPath || '.firestore';

        /** @type {import('./googleSecretManager.service').SecretManager} */
        this.secretManager = this.getInjection("secrets");
        this.secretId = this.config.secret_id || "firestore";

        this.apply_client_id = config.apply_client_id || "none";

        await this.connect();
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

    async connect() 
    {
        if(this.connected)
            return true; 

        // buckets config
        try 
        {            
            debug.log("Connecting to firestore ID="+this.instance());
            
            let serviceAccount = await this.loadConInfo();

            if(!serviceAccount.project_id)
                throw "cant find keys for connecting firestore instance";
                
            const projectId = serviceAccount.project_id;

            // let appName = this.config.application || "default";
            this.dbName = this.config.database || this.config.db || "default";

            this.firebaseApp = admin.initializeApp({
                credential: admin.credential.cert(serviceAccount),
                databaseURL: `https://${projectId}.firebaseio.com`
            });

            // select the database
            this.db = this.getDB(this.dbName);
            
            if(this.db)
                debug.log("Firestore instance connected on project "+serviceAccount.project_id
                    +" with service account "+serviceAccount.client_email);
            else
                throw "cant connect firestore instance";

            this.connected = true;

            debug.log("Connected OK to firestore ID="+this.instance());

            return true;
        }
        catch(err) 
        {
            debug.error(`cant connect to Firestore instance `+err);
            throw err;
            // return Promise.reject({error:500,error:"cant conect to Firebase "+err});
        }
    }

    /**
     * get database by name
     * 
     * @param {*} dbName 
     * @returns {*}
     */
    getDB(dbName = null)
    {
        if(this.dbByName[dbName])
            return this.dbByName[dbName];

        if(dbName && dbName != 'default')
            return this.dbByName[dbName] = getFirestore(this.firebaseApp,dbName);     
        else
            return this.dbByName['default'] = getFirestore(this.firebaseApp);
    }

    async collection(col) {

        await this.connect();

        return this.db.collection(col);
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

      data = formater._formatRecord(data,view);
      let ret = {data};

      if(options.withMeta)
          ret.metadata = view.metadata();

      if(options.withLocale)
          ret.locale = view.locale();

      return ret;
  }

    // map $value => actual value in where string of the form fname = '$value'
 _getFieldWhere(fname,val,tablePrefix=true) 
 {
    if(typeof val == "object" && typeof val.value != "undefined")
        val = val.value;

    if(tablePrefix)
        return (this.desc.wherePrefix[fname] && this.desc.wherePrefix[fname].replace(/[$]val(ue)?/g,val)) || "";
    else
        return (this.desc.whereNoTablePrefix[fname] && this.desc.whereNoTablePrefix[fname].replace(/[$]val(ue)?/g,val)) || "";
    }

  _mapWhere(query,view,coll,withTablePrefix=false) 
  {
      if(query)
      {
        const schema = view.schema();
        const prefix = schema.fieldPrefix();
    
        objectSce.forEachSync(query, (value,name)=> 
        {
            let fw;
            if(value.op && value.value)
            {
                // CASE : value is in the form {op,value}
                // where operand ok (ex. with an array of values)
                fw = {fname:name, op:value.op, value:value.value};
                value = value.value;
            }
            else
                fw = view.getFieldWhere(name,value,withTablePrefix);

            if(fw)
            {
                const fname = fw.fname;
                let op = fw.op || "==";
                if((op == '==' || op == 'in') && value.map)
                {
                    op = "in";
                    coll = coll.where(fname,op,value);      
                }
                else
                {
                    const val = this._formatValue(fw.value,fw.type);
                    coll = coll.where(fname,op,val);      
                }
            }
        });

        return coll;
      }

      if(query && Object.keys(query).length > 0)
      {
        Object.keys(query).forEach((v,fname)=> 
        {
          const val = v.value||v;
          const op = v.operator || "==";
          coll = coll.where(fname,op,val);
        });
      }
  

      return coll;
    }

    // trim white and '
    _formatValue(str,type) 
    {
        if(str.trim)
        {
            str = str.trim();
            if (str.charAt(0) === "'" && str.charAt(str.length - 1) === "'") 
            {
                str = str.substr(1, str.length - 2);
            }        
        }

        if(type == 'integer')
        {
            if (str.charAt(0) === "'" && str.charAt(str.length - 1) === "'") 
            {
                str = str.substr(1, str.length - 2);
            }        
            return parseInt(str);
        }

        return str;
    }

    /** 
     *  get collection name, by using standard collection from model + adding a clientId prefix/suffix
     *  if provided.
     * 
     *  Details :
     *   If a client_id is provided in options or by the model, then add a suffix or prefix to the collection name.
     *   The db needs a "apply_client_id = "coll_suffix", "coll_prefix" for the client id to be applied.
     * 
     * @param {Object} options db options
     * @param {*} model db model
     * @returns {{colName:string,dbName:string}}
     */
    _getCollectionName(options,model) 
    {
        let col = options.collection || model.collection() || this.config.table|| this.config.collection;

        if(!model.getClientId)
            debug.error("missing model.getClientId => check @nxn/db version");

        const clientId = options.client_id || model.getClientId();

        if(clientId && this.apply_client_id != "none")
        {
            if(this.apply_client_id == "coll_prefix")
                col = clientId+"-"+col;
            else if(this.apply_client_id == "coll_suffix")
                col = col + "@" + clientId;
        }

        let dbName = null;
        if(clientId && this.clientManager)
        {
            let cltInfos = this.clientManager.getClientInfosById(clientId);
            let inst = this.id();
            if(cltInfos[inst] && cltInfos[inst].database)
            {
                dbName = cltInfos[inst].database;
            }
        }

        return {colName:col,dbName:dbName||'default'};
    }
    
    /**
     * get firestore collection, based on model/schema and client_id
     * @param {*} options 
     * @param {*} model 
     * @returns {*}
     */
    getCollection(options,model) 
    {
        // get collection name (with client_id rules)
        const {colName,dbName} = this._getCollectionName(options,model);

        // get db (with client_id rules)
        const db = this.getDB(dbName);

        // get collection by name
        let coll = db.collection(colName);

        return {coll,db};
    }
    

    // map field OP value
    fieldWhere(fname,op='=',valueStr='$value',field) 
    {
        op = mapNoSQLOp(op);

        return new WhereExpression(fname,op,valueStr,field.type()||"string");
    }
  
    async findOne(query,options={},model=null) 
    {
        const view = model ? model.getView(options.view||options.$view) : null;
        const orderBy = options.orderBy || null;

        try 
        {
            await this.connect();

            // get db collection (based on model/schema/client_id)
            let {coll} = this.getCollection(options,model);
        
            let data;        
            if (query.id) 
            {
                // search by ID
                try 
                {
                    const doc = await coll.doc(query.id).get();
                    if(doc.exists)
                        data = doc.data();
                    else
                        return null;
                    } 
                    catch (error) 
                    {
                        debug.error(error);
                        return null;  
                    }
                }
            else
            {
                // search by data query
    
                // where
                if(query && Object.keys(query).length > 0)
                {
                    coll = this._mapWhere(query,view,coll);
                }
        
                // page limit
                coll = coll.limit(1); 
            
                // sort
                if(orderBy && orderBy.length > 0)
                {
                    orderBy.forEach((fname)=> 
                    {
                        coll = coll.orderBy(fname);
                    });
                }
            
                // exec
                try 
                {
                    const snap = await coll.get();
                    const docs = snap.docs;
            
                    if(docs.length == 0)
                        return null;
    
                    let doc = docs[0];
                
                    data = doc.data();
                    // data = this._formatRecord(data,view);
                    // data._id = doc.id;
                } 
                catch (error) 
                {
                    debug.error(error);
                    return null;  
                }
            }
          
            let ret = {data};
      
            if(options.withMeta)
                ret.metadata = view.metadata();
                
            if(options.withLocale)
                ret.locale = view.locale();
                
            return ret;            
        } 
        catch (error) {
            
        }
    }    

  async find(query,options,model) 
  {
    const view = model ? model.getView(options.view||options.$view) : null;
    const orderBy = options.orderBy || null;
    const cb = options.cb || null;

    await this.connect();

    // get db collection (based on model/schema/client_id)
    let {coll} = this.getCollection(options,model);

    let ret;

    if(options.test) 
    {
        let where = {}

        for (let p in query)
        {
            where.key = p;
            where.value = query[p];
        }

        let snap = await coll.where(where.key,"==",where.value).get();
        if(!snap.empty)
        {
            snap.forEach(doc => 
            {
                let data = doc.data();            
                console.log("TEST WHERE RESULT =",data);
            });
        }
    }

    if (query.id) 
    {
        // search by ID
        try 
        {
            let data;        

            const doc = await coll.doc(query.id).get();
            if(doc.exists)
                data = doc.data();
            else
                return null;

            ret = {data:[data]};
        } 
        catch (error) 
        {
            debug.error(error);
            return null;  
        }
    }
    else
    {
        // where
        if(query && Object.keys(query).length > 0)
        {
            coll = this._mapWhere(query,view,coll);
        }

        // page limit
        let skip,limit;
        if(options.limit)
        {
            limit = parseInt(options.limit);
            if(limit)
            {
                coll = coll.limit(limit); 

                skip = options.skip||0;
                if(skip)
                    coll = coll.startAt(skip);
            }
        }

        // sort
        if(orderBy && orderBy.length > 0)
        {
            orderBy.forEach((orderByExpr)=> {
                const aOrderBy = orderByExpr.split(' ');
                const fname = aOrderBy[0];
                const direction = aOrderBy.length>1 && (aOrderBy[1]=='desc') && 'desc' || 'asc';
                coll = coll.orderBy(fname,direction);
            });
        }

        // exec query
        const snap = await coll.get();
    
        let results = [];
        if (!snap.empty) snap.forEach(doc => 
        {
            let data = doc.data();
            data._id = doc.id;
            results.push(data);
            if(cb)
                cb(doc,data);
        });

        ret = {data : results};

        let pages = null;
        if(pages)
            ret.pages = pages;
    }

    if(options.withMeta)
        ret.metadata = view.metadata();

    if(options.withLocale)
        ret.locale = view.locale();

    return ret;
  }

  async count(query,options,model) 
  {
    const view = model ? model.getView(options.view||options.$view) : null;

    await this.connect();

    // get db collection (based on model/schema/client_id)
    let {coll} = this.getCollection(options,model);

    // where
    if(query && Object.keys(query).length > 0)
    {
        coll = this._mapWhere(query,view,coll);
    }

    const snap = await coll.get();
    const n = snap.size;
    
    return n;
  }
  
  async insertOne(doc,options,model) 
  {
    const view = model ? model.getView(options.view||options.$view) : null;

    await this.connect();

    // get db collection (based on model/schema/client_id)
    let {coll} = this.getCollection(options,model);

    try 
    {
        let data = {...doc}; // clean class proto not accepted by Firestore
        let id = doc.id || doc._id || doc.oid;
        if(id)
        {
            const item = await coll.doc(id);
            item.set(data);
            return id;    
        }
        else
        {
            const item = await coll.doc();
            item.set(data);
            return item.id;
        }
    } 
    catch (error) 
    {
        debug.error(error);
        return null;
    }
  }

  async insertMany(docs,options,model)
  {
    if(docs.length == 0)
        return null;

    const view = model ? model.getView(options.view||options.$view||"record") : this.config;

    // get db collection (based on model/schema/client_id)
    let {coll,db} = this.getCollection(options,model);

    await this.connect();

    var batch = db.batch();
    docs.forEach((doc) => 
    {
        let data = {...doc}; // clean class proto not accepted by Firestore
        batch.set(coll.doc(), data);
    });

    // Commit the batch
    let res = await batch.commit();

    return res;
  }

  async updateOne(query,data,addIfMissing=false,options,model)
  {
    const view = model ? model.getView(options.view||options.$view) : this.config;

    try 
    {
        // get db collection (based on model/schema/client_id)
        let {coll} = this.getCollection(options,model);

        let res = await this.find(query,options,model);
    
        if(res && res.data) 
        {
            let doc = res.data[0];
            let id = doc.id || doc._id || doc.oid;
            let fields = view.fields();
    
            // clean/complete document
            let doc2 = {...data};
            /*
            let doc2 = {};
            objectService.forEachSync(fields,(field,fname) => 
            {            
                if(typeof doc[fname] == "undefined")
                    doc2[fname] = field.default();
                else
                    doc2[fname] = data[fname];
            });
            */
    
            await coll.doc(id).update(doc2);
        }        
        return 1;
    } 
    catch (error) 
    {
        debug.error(error.message|| error);
        return 0;
    }
  }

    async updateMany(query,docs,addIfMissing=true,options, model) 
    {
        const view = model ? model.getView(options.view||options.$view) : this.config;

        if(docs.length == 0)
            return null;

        let res = await this.find(query,options,model);

        // get db collection (based on model/schema/client_id)
        let {coll,db} = this.getCollection(options,model);

        let fields = view.fields();

        if(docs) 
        {
            var batch = db.batch();
            docs.forEach((doc) => 
            {
                // clean/complete document
                let doc2 = {...doc};
                /*
                let doc2 = {};
                fields.forEach((field,fname) => 
                {            
                    if(typeof doc[fname] == "undefined")
                        doc2[fname] = field.default();
                    else
                        doc2[fname] = doc[fname];
                });
                */

                let id = doc.id || doc.oid;
                batch.update(coll.doc(id), doc2);
            });

            // Commit the batch
            let res = await batch.commit();
            return res;
        }

        return null;
    }

    async deleteOne(query,options, model) 
    {
        let res = await this.findOne(query,options,model);

        try 
        {
            // get db collection (based on model/schema/client_id)
            let {coll} = this.getCollection(options,model);

            if(res && res.data) 
            {
                let doc = res.data;
                let id = doc.id || doc.oid;
        
                await coll.doc(id).delete();
            }
                
        } 
        catch (error) 
        {
            debug.error(error.message);
        }
    }
  
    async deleteMany(query,options, model) 
    {
        await this.connect();

        // get db collection (based on model/schema/client_id)
        let {coll,db} = this.getCollection(options,model);

        const batch = db.batch();

        options.cb = (doc) => 
        {
            batch.delete(doc.ref);
        }

        let res = await this.find(query,options,model);

        if(res && res.data) 
        {
            await batch.commit();
            return res.data.length;
        }
        else
            return 0;
    }  
}

/**
 * 
 * Factory for building a service instance. Each instance has its onwn config.
 * 
 */
class FireStoreFactory
{
  constructor() {
      this.config = {};
  }

  getInstance(name) {
    let config = {};

    if(this.config.instances && this.config.instances[name])
        config = this.config.instances[name];
    else
        config = this.config;

    return new FireStoreInstance(name);
  }
}

module.exports = new FireStoreFactory();



//============ OTHER FUNCTIONS ==================

class WhereExpression {
    fname;
    op;
    value;
    type;
    constructor(fname,op,value,type) {
        Object.assign(this,{fname,op,value,type});
    }

    replace(reg,val) {
        this.value = this.value && this.value.replace(reg,val);
        return this;
    }
}

const _OP_ = {
    "=":"==",
    "==":"==",
    ">=":">=",
    "<=":"<=",
    "!=":"!=",
    "EQ":"==",
    "NEQ":"!=",
}

function mapNoSQLOp(op) {
    return _OP_[op] || op;
}