const debug = require("@nxn/debug")('DbModel');
const {objectSce} = require("@nxn/ext");

//const { Timestamp } = require("mongodb");
const invalidParam = (s) => { throw new Error(s); }
const FlowNode = require("@nxn/boot/node");

const {DbDecorator} = require("./db_decorator.service");
const {Locale, Lang_Locale} = require("./locale.service");

/** field metadata including name, type, etc. */
class SchemaField 
{
    /**
     * 
     * @param {string} name 
     * @param {*} desc 
     * @param {Lang_Locale} locale 
     */
    constructor(name,desc,locale=null) {
        
        if(locale)
            desc.label = locale.f_(name);
        else
            desc.label = desc.label || (name.charAt(0).toUpperCase() + name.slice(1).replace(/_/g,' '));

        this._desc = desc;
        this._name = name;
        this._meta = {...desc};
        ['sqlName','dbName','dbFieldPrefix'].forEach(fn=> {if(this._meta[fn]) delete this._meta[fn] });
    }

    isEnum() {
        return false;
    }

    name() {
        return this._name;
    }

    desc() {
        return this._desc;
    }

    metadata() {
        return this._meta;
    }

    type() {
        return this._prop("type","String");
    }

    required() {
        return this._prop("required",false);
    }

    default() {
        return this._prop("default",null);
    }

    enum() {
        return this._prop("enum",null);
    }

    enumUrl() {
        return this._prop("dataUrl",null);
    }

    label() {
        return this._prop("label") || this.name();
    }

    description() {
        return this._prop("description") || this.name()+" : "+this.type();
    }

    sqlName() {
        return this._prop("sqlName")|| this.name();
    }

    dbName(prefix=null) {
        let n = this._prop("dbName");

        if(!n)
        {
            n = (prefix!==null && prefix) || this.dbFieldPrefix();
            n += this.name();
        }

        return n;
    }

    dbFieldPrefix() {
        return this._prop("dbFieldPrefix")|| '';
    }

    alias() {
        return this._prop("alias")|| this.name();
    } 
    
    tags() {
        return this._desc['x-tags'] || [];
    }

    hasTag(t) {
        return this.tags().includes(t);
    }

    _prop(name,dft=null) {
        if(typeof this._desc[name] != "undefined")
            return this._desc[name];

        return dft;
    }

    static build(name,desc,locale=null) {
        if(desc.enum || desc.enumValues || desc['x-dynamic-values'])
            return new SchemaFieldEnum(name,desc,locale);

        return new SchemaField(name,desc,locale);
    }
}

/** field metadata for enums (static or dynamic) */
class SchemaFieldEnum extends SchemaField {
    constructor(name,desc,locale=null) {
        super(name,desc,locale);

        if(desc.enum && locale)
            this._meta.enum = this.enum = locale.enumList(name,desc.enum || {});
        else
            this.enum = desc.enum || {};
    }

    isEnum() {
        return true;
    }

    getEnum(v,sep=",",locale) {
        if(v.indexOf && v.indexOf('|') > -1)
        {
            let aV = v.split("|").filter(e => e != '').map(e => this._mapEnum(e,locale));
            if(sep)
                return aV.join(sep);

            return aV;
        }

        return this._mapEnum(v,locale);
    }

    _mapEnum(v,locale) {
        try 
        {
            return locale && locale.e_(v,this._name,this.enum && this.enum[v]);
            
        } 
        catch (error) 
        {
            debug.error(error.message);
            throw error;            
        }
    }
}


/** a view is based on the schema definition and also on specific model properties
 *  A view defined in the schema, can have different properties (ex. field names of the database) 
 *  for each model instance.
 */
class DbView 
{
    /**
     * 
     * @param {string} name 
     * @param {*} desc 
     * @param {DbModelInstance} model 
     * @param {string} lang 
     */
    constructor(name,desc,model,lang=null) 
    {
        this.desc = desc;
        this._name = name;
        this._model = model;
        const schema = this._schema = model.schema();
        const locale = this._locale = model.locale(lang);
        this._db = model.db();

        // get fields in config or from schema
        const meta = this._schema.metadata();

        const prefix = this._dbFieldPrefix = desc.dbFieldPrefix || model.fieldPrefix();
        const aPrefix = prefix.split('.');
        if(aPrefix.length>1)
        {
            // prefix includes table name F._
            this._dbTableAlias = aPrefix[0];
            this._dbFieldPrefixNoTable =  aPrefix[1];
        }
        else
        {
            // simple field prefix _
            this._dbTableAlias = null;
            this._dbFieldPrefixNoTable = prefix;
        }

        this._fields = {};
        this._fnames = "";
        let aFnames = [];

        if(typeof desc.fields == "string") 
        {
            // CSV => get field description from schema
            // and "otherFields" list for completing/modifying schema props
            aFnames = this._setFieldFromCSV(desc,schema,locale,prefix);
        }
        else 
        {
            let fields;

            // get fields
            if(typeof desc.fields == "object")
                fields = desc.fields;
            else if(typeof desc.fields == "function")
                fields = desc.fields();

            // complete with otherFields collection
            let otherFields = desc.otherFields || desc["other-fields"];

            if(typeof otherFields == "object")
                fields = {...fields,...otherFields};

            if(fields)
            {
                // get fields from config
                aFnames = this._setFieldsFromDesc(fields,locale,prefix);
            }
        }

        // defines how to format where clauses to be used by db hnadlers
        this.fieldWhere = 
            // check if db handler provides a specific function for formating where operation
            this._db.fieldWhere 
                ||
                // if not, use a simple <fname> <op> <value> formating (works for sql and no sql) 
                ((fname,operator='=',valueStr='$value',field) => 
                {
                    return fname +" "+ operator +" '$value'";
                });

        // get where clause
        let w = {...desc.where};
        this.desc.wherePrefix = this._getWhere(w,prefix); // with full table/field prefix aka T1._
        this.desc.whereNoTablePrefix = this._getWhere(w,this._dbFieldPrefixNoTable); // with only field prefix aka _


        // get internal db names with prefix if any
        this._dbFnames = aFnames.map(n=>
            this._fields[n].dbName(prefix)+' AS `'+this._fields[n].alias()+'`'
        ).join(',');
        this._dbFnamesInsert = aFnames.map(n=>this._fields[n].dbName(this._dbFieldPrefixNoTable)).join(',');
        this._dbFnamesUpdateArray = aFnames.map(n=>this._fields[n].dbName(this._dbFieldPrefixNoTable));

        /*
        if(!prefix)
            this._dbFnames = this._fnames;
        else if(schema.outputWithoutPrefix())
            this._dbFnames = aFnames.map(n=>this._fields[n].dbName()+' AS '+n).join(',');
        else
            this._dbFnames = aFnames.map(n=>prefix+n).join(',');
        */

        // format output record
        if(this.desc.format) {
            this._format = this.desc.format;
        }

        // add metadata infos
        if(this._metadata)
            this._metadata.view = this.infos();
    }

    _setFieldFromCSV(viewDesc,schema,locale,prefix) 
    {
        let fmeta = {};
        let otherFnames=[];

        // CSV => get field description from schema

        // fields: "*"
        if(viewDesc.fields == '*' || viewDesc.fields == '')
            viewDesc.fields = schema.fieldsNames();

        let aFnames = viewDesc.fields.split(',').map(n=>n.trim());

        let otherFields = viewDesc.otherFields || viewDesc["other-fields"];
        if(otherFields && typeof otherFields == "object") 
        {
            otherFnames = Object.keys(otherFields);
            // get unique array from fields and otherFields
            aFnames = [...new Set([...aFnames,...otherFnames])];
        }

        // extract field names
        this._fnames = aFnames.join(',');
        aFnames = this._fnames.split(',');

        // get field descs from shema
        this._fnames.split(',').map(n1=>
        {
            // get schema field
            const n = n1.trim();
            let schemaF = schema.field(n);
            let field2;

            // get field desc from "otherFields" list (allow to complete or modify schema fields descs)
            if(otherFields && otherFields[n])                
                field2 = Object.assign({},otherFields[n]);
            else if(schemaF)
                // or from schema
                field2 = Object.assign({},schemaF.desc());
            else 
            {
                // or error
                let msg = "Unknown field "+n+" in schema for view "+this._name+" of model "+schema.name();
                debug.error(msg);
                throw new Error(msg);
            }

            // add field prefix
            if(prefix)
                field2.dbFieldPrefix = prefix;

            // and set locale version of label
            /*if(locale)
                field2.label = locale.f_(n1);
            */

            // rebuild schema field from new desc
            this._fields[n] = SchemaField.build(n,field2,locale);
            fmeta[n] = this._fields[n].metadata();
        });

        this._metadata = {fields:fmeta};

        // update field name list (with otherFields)
        this._fnames = aFnames.join(",");                
        aFnames = Object.keys(fmeta);

        return aFnames;
    } 

    _setFieldsFromDesc(fields,locale,prefix) 
    {
        let fmeta = {};
        let aFnames = [];

        // get fields from config
        this._fnames = "";

        // Object => get fields from description
        objectSce.forEachSync(fields,(field,fname)=>
        {
            let field2 = Object.assign({},field);

            if(prefix && !field.dbFieldPrefix)
                field2.dbFieldPrefix = prefix;

            if(locale)
                field2.label = locale.f_(fname);
            else
                if(!field2.label)
                    field2.label = this._fields[fname].label();

            this._fields[fname] = SchemaField.build(fname,field2,locale);

            fmeta[fname]=this._fields[fname].metadata();

            aFnames.push(fname);
        });

        aFnames = Object.keys(fmeta);

        this._fnames = aFnames.join(",");
        this._metadata = {fields:fmeta};

        return aFnames;
    }

    _getWhere(whereDesc,fieldPrefix) {
        let where;

        if(!whereDesc)
            where = aFnames.map(n=> 
            {
                const field = this._fields[n] || this._schema.field(n);
                return this.fieldWhere(this._fields[n].dbName(fieldPrefix),"=",'$value',field);
            });
        else 
        {
            where = {};
            objectSce.forEachSync(whereDesc,(v,n)=> 
            {
                if(typeof v == "string")
                    where[n]=v;
                else if(v)
                {
                    let field = this._fields[n] || this._schema.field(n);
                    if(field)
                    {
                        //const vpattern = field.type() == 'integer' ? "$val" : "'$val'";
                        const vpattern = "'$val'";
                        where[n]=this.fieldWhere(field.dbName(fieldPrefix),"=",vpattern,field);
                    }
                    else
                        debug.error("unknown field "+n+" in [where] description for view ["+this._name+"] of model ["+this._model.name()+"]");
                }
            });            
        }

        return where;
    }

    locale() {
        return this._locale;
    }

    fieldsNames(isDbName=false) { 
        if(!isDbName)
            return this._fnames; 
        else
            return this._dbFnames; 
    }
    
    fieldsNamesInsert(isDbName=false) { 
        if(!isDbName)
            return this._fnames; 
        else
            return this._dbFnamesInsert; 
    }
    fieldsNamesUpdate(isDbName=false) { 
        if(!isDbName)
            return this._fnames; 
        else
            return this._dbFnamesUpdateArray; 
    }    
    
    fields() {
        return this._fields;
    }
    
    field(fname) {
        if(this._fields[fname])
            return this._fields[fname];
            // return new SchemaField(fname,this._fields[fname]);
        return null;
    }

    fieldPrefix() {
        return this._dbFieldPrefix;
    }
    tableAlias() {
        return this._dbTableAlias || this._schema._collection;
    }

    tableAsAlias() {
        return this._dbTableAlias ? 
            this._schema._collection+' AS '+this._dbTableAlias : 
                this._schema._collection;
    }


    getQuery(name,driver=null) {
        if(this.desc.queries && this.desc.queries[name])
            return this.desc.queries[name];

        return this._schema.getDefaultQuery(name);
    }

    // map $value => actual value in where string of the form fname = '$value'
    getFieldWhere(fname,val,tablePrefix=true) {
        if(typeof val == "object" && val && typeof val.value != "undefined")
            val = val.value;

        if(val?.map)
        {
            if(val.length > 0)
            {
                if(typeof val == "number")
                {
                    val = val.join(",");
                }
                else // if(typeof val == "string")
                {
                    val = "'"+val.join("','")+"'";
                }
            }            
        }

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

    name() {
        return this._name;
    }

    infos() {
        return {
            name: this._name,
            schema:this._schema.name()
        }
    }

    schema() {
        return this._schema;
    }

    model() {
        return this._model;
    }

    metadata() {
        return this._metadata;
    }

    getFieldsFormats() {
        return this._format || null;
    }
}

/**
 * db schema  holds the list of fields, views that are subsets of these fields, and sql requests.
 * 
 * the schema is defined as a json or yaml descriptor.
 */
class DbSchema
{
    constructor(desc) {
        // get meta
        this._desc = desc;
        this._meta = desc.meta || desc;
        this._name = this._meta.name || this._meta.title || 'Object';

        this._views = {};

        // build fields
        this._fields = {};
        this._fieldsMeta = {};

        let fnames = [];
        const fields = desc.fields || desc.properties;

        const _dbFieldPrefix = desc.dbFieldPrefix ||'';
        this._dbFieldPrefix = _dbFieldPrefix;
        this._outputWithoutPrefix = desc.outputWithoutPrefix || true;

        objectSce.forEachSync(fields,(field,fname)=>{
            let field2 = Object.assign({},field);

            if(_dbFieldPrefix && !field.dbFieldPrefix)
                field2.dbFieldPrefix = _dbFieldPrefix;

            this._fields[fname] = SchemaField.build(fname,field2);
            this._fieldsMeta[fname] = this._fields[fname].metadata(); 
            fnames.push(fname);
        });

        this._fnames = fnames.join(',');

        this._defaultView = { fields:fnames.join(',') };
        if(desc.queries)
            this._defaultView.queries = desc.queries;

        // get id
        this._fId = this._meta.id;
        if(!this._fId) 
        {
            const fId = this._fields._id || this._fields.id || this._fields._oid || this._fields.ID;
            if(fId)
                this._fId = fId.name();
            else
                invalidParam("No id field in schema "+this._name);
        }

        // collection/table
        this._collection = this._meta.table || this._meta.collection || this._meta.name || invalidParam("No collection in data schema");
    }

    name() {
        return this._name;
    }

    collection() {
        return this._collection;
    }

    prop(n,dft=null) {
        if(typeof this._desc[n] != "undefined")
            return this._desc[n];
        else
            return dft;
    }

    fId() {
        return this._fId;
    }

    fields(tag=null) { 
        if(!tag)
            return this._fields;

        let fields={};
        for(let n in this._fields)
        {
            let f = this._fields[n];
            if(f.hasTag(tag))
                fields[n]=f;
        }
        return fields;
    }

    fieldsNames() { 
        return this._fnames; 
    }

    field(fname) {
        if(this._fields[fname])
            return this._fields[fname];
            // return new SchemaField(fname,this._fields[fname]);
        return null;
    }

    fieldPrefix() {
        return this._dbFieldPrefix;
    }

    outputWithoutPrefix() {
        return this._outputWithoutPrefix;
    }

    description() {
        return this._meta.description || "Object "+this.name();
    }

    uri() {
        return this._meta.uri || '/'+this.name();
    }

    metadata() {
        return this._fieldsMeta;
        // return this._desc.fields;
    }

    getDefaultQuery(n) {
        return this._desc.queries && this._desc.queries[n] || null;
    }

    hasView(n) {
        return this._desc.views && this._desc.views[n];
    } 
      
    getViewDesc(n) {
        if(!this._desc.views)
            return this;

        if(n && this._desc.views[n])
            return this._desc.views[n];
        else if(this._desc.views['default'])
            return this._desc.views['default'];
        else
            return this._desc;
    }
}

/**
 * db model instance that executes queries based on schema views.
 * A schema holds the list of fields, views that are subsets of these fields, and sql requests.
 * The db model execute queries via an interface which is similar to mongodb (find, findOne, etc.).
 */
class DbModelInstance
{
    /** @type {string} */
    _name;

    /** db handler */
    _db;

    /**
     * @type {DbSchema}
     */
    _schema;

    /** @type {string} */
    clientId;
    
    /**
     * @type {DbDecorator}
     */
    decorator;
    
    /** if selected lang, get locale for this lang, else, get multi linguage locale */
    _locale;

    /**
     * @type {string}
     */
    _fId;

    /**
     * @type {string}
     */

    /**
     * 
     */
    _modelManager;

    /**
     * 
     * @param {string} name 
     * @param {*} schema 
     * @param {*} db 
     * @param {*} locale 
     * @param {*} config 
     * @param {*} modelManager 
     * @param {*} clientId = null optional client id to use for data segmentation
     */
    constructor(name,schema,db,locale,config,modelManager,decorator,clientId) 
    {
        this.init(name,schema,db,locale,config,modelManager,decorator,clientId);
    }

    init(name,schema,db,locale,config,modelManager,decorator,clientId = null) 
    {
        if(!config || this._config)
            return;

        this._name = name;
        this._config=config;
        this._db = db;
        this._schema = schema;
        this.clientId = clientId;
        this.decorator = decorator;
        
        // if selected lang, get locale for this lang, else, get multi linguage locale
        this._locale = locale;
    
        this._fId = schema.fId() || '_id';
        this._dbFieldPrefix=config.dbFieldPrefix||schema.fieldPrefix();
        this._modelManager = modelManager;
    }

    locale() {
        return this._locale;
    }

    name() {
        return this._name;
    }

    modelManager() {
        return this._modelManager;
    }

    prop(n,dft=null) {
        return this._schema.prop(n,dft);
    }

    schema() {
        return this._schema;
    }
    db() {
        return this._db;
    }
    config() {
        return this._config;
    }

    fieldPrefix() {
        return this._dbFieldPrefix;
    }

    collection() {
        return this._schema.collection();
    }

    /** returns a client id if exists, or null */
    getClientId() {
        return this.clientId;
    }

    hasView(n) {
        return this._schema.hasView(n);
    }

    getView(n,lang=null) 
    {
        lang = lang || this._locale?.lang;

        const k = n+"__"+(lang||'');

        if(!this._views)
            this._views={};

        if(false && this._views[k])
            return this._views[k];

        const viewDesc = this._schema.getViewDesc(n);
        const view = new DbView(n,viewDesc,this,lang);    

        if(lang)
            this._views[k] = view

        return view;
    }        
    
    createCollection() {
        return this._db.createCollection(this);
    }

    removeFieldsFromData(aFnames, data,metadata=null)
    {
        for(let i = 0; i < aFnames.length; i++)
        {
            let fname = aFnames[i];

            if(typeof data[fname] != "undefined")
            {
                delete (data[fname]);
                if(typeof data[fname+'__html'] != "undefined")
                {
                    delete (data[fname+'__html']);
                }
            }
            if(metadata && typeof metadata.fields[fname] != "undefined")
            {
                delete (metadata.fields[fname]);
            }
        }

        return {data,metadata};
    }

  /* ============ SUPPORT INTERFACE UNIFIEE BASEE SUR MONGODB ================= */
    findById(id,options={}) {
        const where={};
        where[this._fId]=id;
        
        if(this.decorator && this.decorator.findById)
            return this.decorator.findOne(where,options,this,this._db);
        else
            return this._db.findOne(where,options,this);            
    }

    findOne(where={},options={}) 
    {
        if(this.decorator && this.decorator.findOne)
            return this.decorator.findOne(where,options,this,this._db);
        else
            return this._db.findOne(where,options,this);
    }

    loadFromData(doc,options={})
    {
        const model = this;
        const view = model ? model.getView(options.view||options.$view) : null;
        
        let data={};
        objectSce.forEachSync(view.fields(),(f,n) => {
            let type = f.type();

            data[n] = doc[n] ||
                ((type=='string') ? '' :
                (type=='integer') ? 0  : '');
        });

        data = this._db._formatRecord(data,view,false);
        let ret = {data};

        if(options.withMeta)
            ret.metadata = view.metadata();

        if(options.withLocale)
            ret.locale = view.locale();

        return ret;
    }
    
    getEmpty(options={}) 
    {
        if(this.decorator && this.decorator.getEmpty)
            return this.decorator.getEmpty(options,this,this._db);
        else
            return this._db.getEmpty(options,this);
    }

    async find(where={},options={}) 
    {
        if(this.decorator && this.decorator.find)
            return this.decorator.find(where,options,this,this._db);
        else
            return this._db.find(where,options,this);
    }

    async count(where={},options={}) {
        if(this.decorator && this.decorator.find)
            return this.decorator.count(where,options,this,this._db);
        else
            return this._db.count(where,options,this);
    }

    async insertOne(doc,options={}) 
    {
        if(this.decorator && this.decorator.insertOne)
            return this.decorator.insertOne(doc,options,this,this._db);
        else
            return this._db.insertOne(doc,options,this);
    }

    async insertMany(docs,options={}) 
    {
        if(this.decorator && this.decorator.insertMany)
            return this.decorator.insertMany(docs,options,this,this._db);
        else
            return this._db.insertMany(docs,options,this);
    }

    async updateOne(where,doc,options={}) 
    {
        if(this.decorator && this.decorator.updateOne)
            return this.decorator.updateOne(where,doc,false,options,this,this._db);
        else
            return this._db.updateOne(where,doc,false,options,this);
    }

    async updateMany(where,doc,options={}) 
    {
        if(this.decorator && this.decorator.updateMany)
            return this.decorator.updateMany(where,doc,options,this);
        else
            return this._db.updateMany(where,doc,options,this);
    }

    async deleteOne(doc,options={}) 
    {
        if(this.decorator && this.decorator.updateMany)
            return this.decorator.deleteOne(doc,options,this);
        else
            return this._db.deleteOne(doc,options,this);
    }

    async deleteMany(where,options={}) 
    {
        if(this.decorator && this.decorator.updateMany)
            return this.decorator.deleteMany(where,options,this);
        else
            return this._db.deleteMany(where,options,this);
    }  
}

/** db model : loads a record schema (metadata including fields, views etc.) and
 * is attached to a db engine instance. A DB model is used to instanciate db model instances
 * that perfom queries.
 */
class DbModel extends FlowNode
{
    constructor(instName,modelManager) {
        super(instName);
        this._modelManager = modelManager;
    }

    async init(config,ctxt,...injections) {
        super.init(config,ctxt,injections);        

        this._db = this.getInjection('db') || this.invalidParam("no db injection");

        this._schema = new DbSchema(config.schema,this._locale);

        this.decorator = this.getInjection("decorator");

        // store collection name for automatic creation of tables in db
        const coll = this._schema.collection();
        if(coll)
            this._modelManager.registerCollectionModel(coll,this);

        // cache des instances de mod?les (instance d'instance)
        this.instances={};
    }

    uri() {
        return this.config.uri || this._schema.uri();
    }
    
    /**
     * get db schema for this model
     * @returns {DbSchema}
     */
    schema() {
        return this._schema;
    }
    
    db() {
        return this._db;
    }

    /**
     * returns a db model instance. Each instance is unique by :
     * - schema name
     * - lang (optional)
     * - database instance name (based on nxn component id)
     * - client id (optional)
     * 
     * @param {*} lang 
     * @param {*} [clientId=null] 
     * 
     * @returns {DbModelInstance}
     */
    instance(lang=null,clientId=null) 
    {
        if(!clientId)
            clientId = process.env.client_id || process.env.GED_CLIENT_ID || null;

        const name = this._schema.name();
        const dbId = this._db.id && this._db.id() || '';
        const key = name + '_'+(lang || '') + '_'+dbId+'_'+(clientId||'');

        if(!this.instances[key])
        {
            this.instances[key] = new DbModelInstance(
                this._schema.name(),
                this._schema,
                this._db,
                this.locale.localeByLang(lang),
                this.config,
                this._modelManager,
                this.decorator,
                clientId);
        }

        return this.instances[key];
    }

    /**
     * returns a db model instance. Each instance is unique by :
     * - schema name
     * - lang (optional)
     * - database instance name (based on nxn component id)
     * - client id (optional)
     * 
     * @param {{lang,client_id?}} user 
     * @param {*} [clientId=null] 
     * 
     * @returns {DbModelInstance}
     */
    instanceByUser(user) 
    {
        let lang = user?.lang;
        let clientId = user?.client_id || process.env.client_id || process.env.GED_CLIENT_ID || null;

        const name = this._schema.name();
        const dbId = this._db.id && this._db.id() || '';
        const key = name + '_'+(lang || '') + '_'+dbId+'_'+(clientId||'');

        if(!this.instances[key])
        {
            this.instances[key] = new DbModelInstance(
                this._schema.name(),
                this._schema,
                this._db,
                this.locale.localeByLang(lang),
                this.config,
                this._modelManager,
                this.decorator,
                clientId);
        }

        return this.instances[key];
    }    
}

/** factory for instanciating a db model */
class DbModelSce
{
    constructor () {
        this.instances={};
        this.collections={};
    }

    /** return a model instance
     * @returns {DbModel}
     */
    getInstance(instName) {
        if(this.instances[instName])
            return this.instances[instName];

        const inst = this.instances[instName] = new DbModel(instName,this);

        return inst;
    }

    // store a lookup of model by table name for recovery of missing tables/fields
    registerCollectionModel(coll,model) {
        this.collections[coll] = model;
    }

    getModelByCollection(coll) {
        if(this.collections[coll])
            return this.collections[coll];

        return null;
    }
}

const DB_SCE = new DbModelSce();
module.exports = DB_SCE;

module.exports.SchemaField = SchemaField;
module.exports.SchemaFieldEnum = SchemaFieldEnum;
module.exports.DbView = DbView;
module.exports.DbModelInstance = DbModelInstance;
module.exports.DbModel = DbModel;
module.exports.DbModelSce = DbModelSce;
module.exports.DbDecorator = DbDecorator;
