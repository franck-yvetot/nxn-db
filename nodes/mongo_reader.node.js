
const debug = require("@nxn/debug")('Mongo_writer');
const FlowNode = require("@nxn/boot/node");
const MongoService = require("@nxn/db/mongodb.service");

class Mongo_readerNode extends FlowNode
{
    constructor() {
        super();
    }

    async init(config,ctxt,...injections) {
        super.init(config,ctxt,injections);

        this.db = this.getInjection('db');
        this.instance = this.config.instance||null;
        this.coll = this.config.collection||null;

        this.connect(this.instance);
    }

    name() {
        return this.config.name ||"MY_SCRIPT";
    }

    connect(inst) {
        if(this.db)
            return;

        this.db = MongoService.getInstance(inst); 
    }

    async close() {
        if(this.db)
            this.db = await this.db.close();    
        this.db = null;
    }

    async listAll(msg) {
        let page=0;
        let limit=this.config.limit||15;
        let search = this.getParam(msg,'where',{});

        this.connect(this.inst);
        const count = await this.db.count(search,this.coll);

        const nbPages = (count+limit-1)/limit;
        for(let page = 0; page < nbPages;page++)
        {
            await this.list(msg,search,limit,page);
        }
    }

    async list(msg,search,limit=15,page=0) {
        try 
        {
            this.connect(this.inst);

            debug.log("read page #"+page);
            const qs = search; // search;

            const skip = page*limit;
            const rows = await this.db.find(qs,this.coll,limit,skip);

            await arraySce.forEachAsyn(rows, async row=>{

                if(this.canSendMessage()) {
                    try {
                        msg.data = row;
                        await this.sendMessage(msg);
                    } catch (error) {
                        debug.log("ERROR :"+error.message+error.stack);
                    }
                }
            });

            return rows;
        }
        catch(error) {
            let message = error.message || error;
            let code = parseInt(error.code||500, 10) || 500;
            debug.error(error.stack||error);
        }            
    }

    async processMessage(message) {

        try {

            // do something here...
            this.listAll(message);

        }

        catch(error) {
            let message = error.message || error;
            let code = parseInt(error.code||500, 10) || 500;
            debug.error(error.stack||error);
        }        
    }
}

class Doc {
    constructor(obj) {
        this._data = obj;
    }

    data() {
        return this._data;
    }

    static build(obj) {
        if(!obj)
            return null;

        return new Doc(obj);
    }

    static buildFromRow(row) {
        if(!row)
            return null;

        return new Praticien(obj2);
    }
}

class Factory
{
    constructor () {
        this.instances={};
    }
    getInstance(instName) {
        if(this.instances[instName])
            return this.instances[instName];

        return (this.instances[instName] = new Mongo_readerNode(instName));
    }
}

module.exports = new Factory();
