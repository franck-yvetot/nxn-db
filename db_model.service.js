const debug = require("@nxn/debug")('DbModel');
const {objectSce} = require("@nxn/ext");
const { Timestamp } = require("mongodb");
const invalidParam = (s) => { throw new Exception(s); }
const FlowNode = require("@nxn/boot/node");

class SchemaField {
    constructor(name,desc) {
        desc.label = desc.label || (name.charAt(0).toUpperCase() + name.slice(1).replace(/_/g,' '));
        this._desc = desc;
        this._name = name;
        this._meta = {...desc};
        ['sqlName','dbName','dbFieldPrefix'].forEach(fn=> {if(this._meta[fn]) delete this._meta[fn] });
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

    _prop(name,dft=null) {
        if(typeof this._desc[name] != "undefined")
            return this._desc[name];

        return dft;
    }
}

/** a view is based on the schema definition and alos on specific model properties
 *  A view defined in the schema, can have different properties (ex. field names of the database) 
 *  for each model instance.
 */
class DbView {
    constructor(name,desc,model) {
        this.desc = desc;
        this._name = name;
        this._model = model;
        const schema = this._schema = model.schema();
        const locale = this._locale = model.locale();

        // get fields in config or from schema
        const meta = this._schema.metadata();

        const prefix = this._dbFieldPrefix = desc.dbFieldPrefix || model.fieldPrefix();
        let fmeta = {};

        this._fields = {};
        this._fnames = "";
        let aFnames = [];

        if(typeof desc.fields == "string") 
        {
            // CSV => get field description from schema

            if(desc.fields == '*' || desc.fields == '')
                desc.fields = schema.fieldsNames();

            this._fnames = desc.fields.split(',').map(n=>n.trim()).join(',');
            aFnames = this._fnames.split(',');

            this._fnames.split(',').map(n1=>{
                const n = n1.trim();
                let field2 = Object.assign({},schema.field(n).desc());

                if(prefix)
                    field2.dbFieldPrefix = prefix;

                if(locale)
                    field2.label = locale.f_(n1);
    
                this._fields[n] = new SchemaField(n,field2);
                fmeta[n] = this._fields[n].metadata();
            });

            this._metadata = {fields:fmeta};
        }
        else if(typeof desc.fields == "object") 
        {
            // get fields from config

            this._fnames = "";
            let meta = {};

            // Object => get fields from description
            objectSce.forEachSync(desc.fields,(field,fname)=>{
                let field2 = Object.assign({},field);
    
                if(prefix && !field.dbFieldPrefix)
                    field2.dbFieldPrefix = prefix;
    
                this._fields[fname] = new SchemaField(fname,field2);
                if(locale)
                    field2.label = locale.f_(fname);
                else
                    if(!field.label)
                        field.label = this._fields[fname].label();

                meta[fname]=this._fields[fname].metadata();
                aFnames.push(fname);
            });

            this._fnames = aFnames.join(",");
            this._metadata = {fields:meta};
        }

        if(!desc.where)
            this.desc.where = aFnames.map(n=>this._fields[n].dbName(prefix)+" = '$value'");
        else {
            let where = {};
            objectSce.forEachSync(desc.where,(v,n)=> {
                if(typeof v == "string")
                    where[n]=v;
                else if(v)
                {
                    let field = this._fields[n] || schema.field(n);
                    if(field)
                        where[n]=field.dbName(prefix)+" = '$val'";
                    else
                        debug.error("unknown field in [where] description for view "+this._name+" of model "+model.name());
                }
            });
            this.desc.where = where;
        }

        // get internal db names with prefix if any
        this._dbFnames = aFnames.map(n=>this._fields[n].dbName(prefix)+' AS `'+this._fields[n].alias()+'`').join(',');
        this._dbFnamesInsert = aFnames.map(n=>this._fields[n].dbName(prefix)).join(',');

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
    fields() {
        return this._fields;
    }

    fieldPrefix() {
        return this._dbFieldPrefix;
    }

    getQuery(name) {
        if(this.desc.queries && this.desc.queries[name])
            return this.desc.queries[name];

        return this._schema.getDefaultQuery(name);
    }

    getFieldWhere(fname,val) {
        return (this.desc.where[fname] && this.desc.where[fname].replace("$val",val)) || "";
    }

    name() {
        return this._name;
    }

    schema() {
        return this._schema;
    }

    metadata() {
        return this._metadata;
    }

    getFieldsFormats() {
        return this._format || null;
    }
}

class DbSchema
{
    constructor(desc) {
        // get meta
        this._desc = desc;
        this._meta = desc.meta || desc;
        this._name = this._meta.name || 'Object';

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

            this._fields[fname] = new SchemaField(fname,field2);
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
                invalidParam("No id field in schema");
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

    fId() {
        return this._fId;
    }

    fields() { 
        return this._fields; 
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
      
    getViewDesc(n) {
        if(!this._desc.views)
            return this;

        if(n && this._desc.views[n])
            return this._desc.views[n];
        else if(this._desc.views['default'])
            return this._desc.views['default'];
        else
            return this;
    }
}

class DbModelInstance
{
    constructor(name,schema,db,locale,config) {
        this.init(name,schema,db,locale,config);
    }

    init(name,schema,db,locale,config) {
        if(!config || this._config)
            return;

        this._config=config;
        this._db = db;
        this._schema = schema;
        this._locale = locale;
    
        this._fId = schema.fId() || '_id';
        this._dbFieldPrefix=config.dbFieldPrefix||schema.fieldPrefix();
    }

    locale() {
        return this._locale;
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

    getView(n) {
        if(!this._views)
            this._views={};

        if(this._views[n])
            return this._views[n];

        const viewDesc = this._schema.getViewDesc(n);
        this._views[n] = new DbView(n,viewDesc,this);
        return this._views[n];
    }        

  /* ============ SUPPORT INTERFACE UNIFIEE BASEE SUR MONGODB ================= */
    findById(id,options={}) {
        const where={};
        where[this._fId]=id;
        return this._db.findOne(where,options,this);
    }

    findOne(where={},options={}) {
        return this._db.findOne(where,options,this);
    }

    getEmpty(options={}) {
        return this._db.getEmpty(options,this);
    }

    async find(where={},options={}) {
        return this._db.find(where,options,this);
    }

    async count(where={},options={}) {
        return this._db.count(where,options,this);
    }

    async insertOne(doc,options={}) {
        return this._db.insertOne(doc,options,this);
    }

    async insertMany(docs,options={}) {
        return this._db.insertMany(docs,options,this);
    }

    async updateOne(doc,options={}) {
        return this._db.updateOne(doc,options,this);
        //const addIfMissing = !!(options.upsert);
    }

    async updateMany(where,doc,options={}) {
        //const addIfMissing = !!(options.upsert);
        return this._db.updateMany(where,doc,options,this);
    }

    async deleteOne(doc,options={}) {
        return this._db.deleteOne(doc,options,this);
    }

    async deleteMany(where,options={}) {
        return this._db.deleteMany(where,options,this);
    }  
}

class DbModel extends FlowNode
{
    constructor(instName) {
        super(instName);
    }

    async init(config,ctxt,...injections) {
        super.init(config,ctxt,injections);        

        this._db = this.getInjection('db') || this.invalidParam("no db injection");
        this._locale = this.getInjection('locale') || null;
        this._schema = new DbSchema(config.schema,this._locale);
    }

    uri() {
        return this.config.uri || this._schema.uri();
    }
    
    schema() {
        return this._schema;
    }
    
    db() {
        return this._db;
    }

    locale() {
        return this._locale;
    }

    instance() {
        return new DbModelInstance(this._schema.name(),this._schema,this._db,this._locale,this.config);
    }
}

class DbModelSce
{
    constructor () {
        this.instances={};
    }
    getInstance(instName) {
        if(this.instances[instName])
            return this.instances[instName];

        return (this.instances[instName] = new DbModel(instName));
    }
}

module.exports = new DbModelSce();