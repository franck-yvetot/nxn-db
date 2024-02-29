
const debug = require("@nxn/debug")('Mongo_writer');
const FlowNode = require("@nxn/boot/node");
const MongoService = require("@nxn/db/mongodb.service");

class Mongo_writerNode extends FlowNode
{
    constructor() {
        super();
    }

    async init(config,ctxt,...injections) {
        super.init(config,ctxt,injections);

        this.db = this.getInjection('db');
        this.instance = this.config.instance||null;
        this.coll = this.config.collection||null;
    }

    connect(inst) {
        if(this.db)
            return;

        if(!this.db)
            this.db = MongoService.getInstance(inst); 
    }

    async close() {
        if(this.db)
            this.db = await this.db.close();    
        this.db = null;
    }

    async processMessage(msg) 
    {
        try 
        {
            this.connect(this.instance);

            if(msg) 
            {
                if(!msg.data.forEach)
                    await this.db.insertOne(msg.data,this.coll);
                else if(msg.data.length)
                    await this.db.inserMany(msg.data,this.coll);
            }

            await this.sendMessage(msg);
        }
        catch(error) 
        {
            let message = error.message || error;
            let code = parseInt(error.code||500, 10) || 500;
            debug.error(error.stack||error);
        }        
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

        return (this.instances[instName] = new Mongo_writerNode(instName));
    }
}

module.exports = new Factory();