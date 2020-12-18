const debug = require("@nxn/debug")('GDRIVE_PROPS_DB_SCE');
const { objectSce } = require("@nxn/ext");
const gdrive_rest = require("../services/gdrive_rest.service");

class GDrivePropsDBInstance {
    constructor(config) {
        if (config)
            this.init(config);
    }

    async init(config) {
        if (!config || this.config)
            return;

        this.config = config;

        // auth verification class?
        this.auth =  this.getInjection('auth');
        this.gdrive = gdrive_rest.getInstance('docSce');
    }

    async connect(force = false) {
        return true;
    }

    async close() {
        if (this.connected) {
            this.connected = false;
        }
    }

    /* ============ SUPPORT INTERFACE UNIFIEE BASEE SUR MONGODB ================= */

    _encodeData(doc,view) 
    {
        const schema = view.schema();
        let doc2 = {};
        
        for (let fname in doc)
        {
            let label = schema.field(fname).label();
            doc2[label] = doc[fame];
        }

        return doc2;
    }

    async _setData(options,doc,view) {
        if(!options.$data)
            return null;
        
        if(!options.token)
            return null;

        const token  = options.token;
        const fileId = options.$data.uid;

        let props = this._encodeData(doc,view);

        await this.gdrive.setProperties(fileId,props,token,"PUBLIC");
    }

    async createCollection(model) {
        return true;
    }

    async findOne(query, options, model) {

        const view = model ? model.getView(options.view || options.$view || "record") : null;

        let doc = this._getData(options,view);

        let data = this._formatRecord(doc, view);
        let ret = { data };

        if (options.withMeta)
            ret.metadata = view.metadata();

        return ret;
    }

    getEmpty(options, model) {
        return false;
    }

    async find(query, options, model) {
        return false;
    }

    async count(query, options, model) {
        return false;
    }

    async insertOne(doc, options, model) {

        const view = model ? model.getView(options.view || options.$view || "record") : this.config;

        this._setData(options,doc,view);
    
        const insertId = 1;

        return insertId;
    }

    async insertMany(docs, options, model) {
        const view = model ? model.getView(options.view || options.$view || "record") : this.config;

        if (docs.length == 0)
            return null;

        this._setData(options,docs,view);
    
        return true;
    }

    async updateOne(query, doc, addIfMissing = true, options, model) {
        const view = model ? model.getView(options.view || options.$view || "record") : this.config;

        this._setData(options,doc,view);

        return true;
    }

    async updateMany(query, docs, addIfMissing = true, options, model) {    
        return false;
    }

    async deleteOne(query, options, model) {
        return false;
    }

    async deleteMany(query, options, model) {
        return false;
    }
}

class GDrivePropsDBSce {
    constructor() {
        this.instances = {};
    }
    getInstance(instName) {
        if (this.instances[instName])
            return this.instances[instName];

        return (this.instances[instName] = new GDrivePropsDBInstance());
    }
}

module.exports = new GDrivePropsDBSce();