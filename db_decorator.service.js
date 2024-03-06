const { FlowNode } = require("@nxn/boot");

/**
 * Database decorator base class
 * 
 * add a "decorator" injection to db model, to enable the dcorator
 * to be activated. By creating a db decorator, it is possible to 
 * execute some code before or after the requests to the DB.
 * 
 * Examples : to create joint tables, execute calls to external 
 * services before or after requests, or simply measure time taken by requests.
 */
class DbDecorator extends FlowNode
{
    constructor(inst=null) {
        super(null);
    }

    getEmpty(options,model,db) 
    {
        return db.getEmpty(options,model);
    }

    async findOne(query,options,model,db) 
    {
        return db.findOne(query,options,model);
    }

    async find(query,options,model,db)
    {
        return db.find(query,options,model);
    }

    async count(query,options,model,db)
    {
        return db.count(query,options,model);
    }    

    async insertOne(doc,options,model,db) 
    {
        return db.insertOne(doc,options,model);
    }

    async insertMany(docs,options,model,db) 
    {
        return db.insertMany(docs,options,model)
    } 

    async updateOne(query,doc,addIfMissing=false,options,model,db)
    {
        return db.updateOne(query,doc,addIfMissing,options,model)
    }

    async deleteOne(query,options, model,db) 
    {
        return db.deleteOne(query,options, model);
    }

    async deleteMany(query,options, model,db) 
    {
        return db.deleteMany(query,options, model);
    }
}

module.exports = DbDecorator;
module.exports.DbDecorator = DbDecorator;