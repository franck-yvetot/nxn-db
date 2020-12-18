const debug = require("@nxn/debug")('FIELD_DB_SCE');
const { objectSce } = require("@nxn/ext");

class AsFieldDBInstance {
    constructor(config) {
        if (config)
            this.init(config);
    }

    async init(config) {
        if (!config || this.config)
            return;

        this.config = config;
        this.propertyName = config.property_name || 'x_data';
        this.dataFormat = 'json_b64';
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
    _formatRecord(rec, view) {
        const format = view.getFieldsFormats();
        const locale = view.locale();

        if (!format)
            return Object.assign({}, rec);

        objectSce.forEachSync(format, (vcsv, k) => {
            let av = vcsv.split(',');
            for (let i = av.length - 1; i >= 0; i--) {
                // execute in reverse order
                let v = av[i].trim();
                const func = "_format_" + v;
                if (typeof this[func] == "function")
                    this[func](k, rec, locale);
            }
        });

        return rec;
    }

    _format_json(fname, rec) {
        if (rec[fname]) {
            rec[fname] = JSON.parse(rec[fname]);
        }
    }

    _format_base64(fname, rec) {
        if (rec[fname]) {
            rec[fname] = Buffer.from(rec[fname], 'base64').toString('utf8');
        }
    }

    _format_enum(fname, rec) {
        if (rec[fname] && typeof (rec[fname + '__html']) != "undefined") {
            rec[fname] = {
                value: rec[fname],
                html: rec[fname + '__html']
            };
            delete rec[fname + '__html'];
        }
        else
            return { value: rec[fname], html: '' };
    }

    _format_enum_static(fname, rec, locale) {
        if (rec[fname]) {
            rec[fname] = {
                value: rec[fname],
                html: (locale && locale.e_(rec[fname], fname)) || rec[fname]
            };
        }
    }

    _formatLocaleValues(fname, rec) {
        rec[fname] = this._locale.v_(rec[fname]);
    }

    _parseValue(v, field) {
        if (v && typeof v.value != "undefined")
            v = v.value;

        if (v === null || typeof v == "undefined")
            return "NULL";

        const type = field.type();
        if (type == 'string')
            if (v.replace)
                return "'" + v.replace(/'/g, "\\'") + "'";
            else
                return v;

        if (type == 'integer')
            return 0 + v;

        if (type == 'date') {
            if (typeof v == "integer")
                return v;
            if (v.includes && v.includes("NOW"))
                return v.replace(/now(\s*[(]\s*[)])?/i, 'NOW()');

            v = v.split('T')[0];

            return v;
        }

        if (type == 'timestamp') {
            if (typeof v == "integer")
                return v;
            if (v.includes && v.includes("NOW"))
                return v.replace(/now(\s*[(]\s*[)])?/i, 'NOW()');
                
            return v;
        }

        return "'" + v + "'";
    }

    _setData(options,v) {
        if(!options.$data)
            return null;

        let data = this._encodeData(v);

        options.$data[this.propertyName] = data;

        return data;    
    }

    _getData(options,view) {
        if(!options.$data || !options.$data[this.propertyName])
            return {};

        // get data and decode
        let data;
        try {
        let rawData = options.$data[this.propertyName];
            data = this._decodeData(rawData) || {};    
        }
        catch(err) {
            debug.error("Meta data JSON corrupted");
            data = {};
        }

        // filters data
        let data2 = {};
        let fields = view.fields();
        for (let fname in fields)
        {
            if(data[fname])
                data2[fname] = data[fname]; 
        }

        return data2;
    }
    _getDataMul(options,view) {
        if(!options.$data || !options.$data[this.propertyName])
            return [];

        let rawData = options.$data[this.propertyName];
        let data = this._decodeData(rawData) || [];
        
        return data;
    }

    _encodeData(v) {
        let json = JSON.stringify(v);
        let b64 = Buffer.from(json).toString('base64');
        return b64;
    }
    _decodeData(data) {
        let json = Buffer.from(data, 'base64').toString('utf8');
        let v = JSON.parse(json);
        return v;
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
        const view = model ? model.getView(options.view || options.$view) : null;

        let data = {};
        objectSce.forEachSync(view.fields(), (f, n) => {
            data[n] =
                (f.type == 'string') ? '' :
                    (f.type == 'integer') ? 0 :
                        '';
        });

        data = this._formatRecord(data, view);
        let ret = { data };

        if (options.withMeta)
            ret.metadata = view.metadata();

        return ret;
    }

    async find(query, options, model) {
        const view = model ? model.getView(options.view || options.$view) : null;

        let data = this._getDataMul(options);

        if (this.config.log)
            debug.log(qs + " -> nb = " + data.length + " / $view=" + view.name());

        const nb = data.length;
        let pages = null;
        if (nb) {
            pages = {
                offset: skip,
                limit: limit,
                total: nb
            };
        }

        // remap fields?
        if (view.getFieldsFormats()) {
            data = data.map(rec => this._formatRecord(rec, view));
        }

        let ret = { data, pages };

        if (options.withMeta)
            ret.metadata = view.metadata();

        return ret;
    }

    async count(query, options, model) {
        const view = model ? model.getView(options.view || options.$view) : this.config;

        if (this.config.log)
            debug.log(qs + " / $view=" + view.name());

        let data = this._getDataMul(options);
    
        const docs = data.length;
        return docs;
    }

    async insertOne(doc, options, model) {

        const view = model ? model.getView(options.view || options.$view || "record") : this.config;

        this._setData(options,doc);
    
        const insertId = 1;

        return insertId;
    }

    async insertMany(docs, options, model) {
        const view = model ? model.getView(options.view || options.$view || "record") : this.config;

        if (docs.length == 0)
            return null;

        this._setData(options,docs);
    
        return true;
    }

    async updateOne(query, doc, addIfMissing = true, options, model) {

        this._setData(options,doc);

        return true;
    }

    async updateMany(query, docs, addIfMissing = true, options, model) {

        if (docs.length == 0)
            return null;

        this._setData(options,docs);
    
        return true;
    }

    async deleteOne(query, options, model) {

        this._setData(options,null);

        return true;
    }

    async deleteMany(query, options, model) {
        this._setData(options,null);

        return true;
    }
}

class AsFieldDBSce {
    constructor() {
        this.instances = {};
    }
    getInstance(instName) {
        if (this.instances[instName])
            return this.instances[instName];

        return (this.instances[instName] = new AsFieldDBInstance());
    }
}

module.exports = new AsFieldDBSce();