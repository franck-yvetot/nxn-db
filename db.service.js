// which db type to use, default to mongo
const mongoose = require("mongoose");
const debug = require("@nxn/debug")("DB");

class DbService 
{
    constructor() {
        this.dbType = null;
        this.connected = false;
    }

    init(config,app,express) {
        const dbType = config.dbType;
        this.connect(dbType);
    }

    // connect to db
    connect(dbType) {
        var self = this;

        this.dbType = process.env.DB_TYPE||'mongo';

        if(!dbType)
            dbType = this.dbType;

        if(dbType == 'mongo')
        {               
            // connect to db
            mongoose.connect(
                process.env.MONGO_CONN,
                {useNewUrlParser:true},
                (err)=> { 
                    if(err)
                        debug.error('connection failed to db : '+err.message);
                    else 
                    {
                        debug.log('connected to db');
                        self.connected = true;
                    }
                }
            );

            return;
        }

        debug.error('Cant connect to db type : '+dbType+ ', connector not yet implemented...');
    }

    // get object model for use with connected db
    getModel(dataSchema,dbType) {
        var model;

        if(!this.connected)
            this.connect();

        if(!dbType)
            dbType = this.dbType;
        
        if(dbType == 'mongo')
        {   
            // instanciate mongo model
            const mongoSchema = new mongoose.Schema(dataSchema.fields);
            const model = mongoose.model(dataSchema.name,mongoSchema);
            return model;
        }
        else if(dbType == 'firebase')
        {
            var firebase = require("firebase");
        }
        else
        {
        }
        return null;        
    }

    jsonSchemaToMongoose(jsonSchema) {
        let dataSchema = jsonSchema.properties;
        return dataSchema;
    }
}

module.exports = new DbService();