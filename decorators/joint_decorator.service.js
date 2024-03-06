// @ts-check
const debug = require("@nxn/debug")('Tag_decorator');
const {DbModel, DbDecorator} = require("@nxn/db/db_model.service");

/** my service description here */
class JointTable_decoratorSce extends DbDecorator
{
    /**
     * @type {DbModel}
     */
    jointTableModel;

    /** @type {string} */
    key;

    /**
     * foreign key 
     * @type {string} */
    fkey = "oid";

    /** init the service with a config */
    async init(config,ctxt,...injections) {
        super.init(config,ctxt,injections);
    }

    async findOne(query,options,model,db) 
    {
        return db.findOne(query,options,model);
    }

    /**
     * 
     * @param {*} doc 
     * @param {*} options 
     * @param {DbModel} model 
     * @param {*} db 
     * @returns 
     */
    async insertOne(doc,options,model,db) 
    {
        let doc_oid = await db.insertOne(doc,options,model);
        let values;
        let records;

        try 
        {
            if(doc[this.key] && doc[this.key].value)
            {
                const jointInst = this.jointTableModel.instance("Joint");
    
                values = doc[this.key].value;
                if(values)
                {
                    records = [];
                    for (let v of values.split("|")) 
                    {
                        if(!v)
                            continue;
    
                        // add record joints
                        records.push({
                            key:doc_oid,   
                            fkey:v
                        });
                    }
    
                    // insert in asynchrone
                    jointInst.insertMany(records);
                }
            }                
        } 
        catch (error) 
        {
            debug.error(error.message);
        }

        return doc_oid;
    }

    /**
     * 
     * @param {*} query 
     * @param {*} doc 
     * @param {*} addIfMissing 
     * @param {*} options 
     * @param {DbModel} model 
     * @param {*} db 
     * @returns 
     */
    async updateOne(query,doc,addIfMissing=false,options,model,db)
    {
        let doc_oid = doc.oid;

        await db.updateOne(query,doc,addIfMissing,options,model);

        let values;
        let records;

        try 
        {
            if(doc[this.key] && doc[this.key].value)
            {
                // get existing values
                const jointInst = this.jointTableModel.instance("TagJoint");
                records = await jointInst.find({doc_oid});
                let existingRecs = records?.data;
                let existingLookup = {}
                if(existingRecs)
                {
                    // create record lookup[tag_oid]
                    for(let i = 0; i < existingRecs.length; i++)
                    {
                        let rec = existingRecs[i];
                        existingLookup[rec[this.fkey]] = rec;
                    }                    
                }
    
                // check/add new values
                values = doc[this.key].value;
                if(values)
                {
                    records = [];
                    for (let v of values.split("|")) 
                    {
                        if(!v)
                            continue;

                        if(!existingLookup[v])
                        {
                            // new tag
                            
                            // add record joints
                            records.push({
                                key:doc_oid,   
                                fkey:v
                            });
                        }
                        else
                            // already in db
                            existingLookup[v]._keepIt = true;
                    }
    
                    // insert new entries (asynchrone)
                    if(records.length)
                        jointInst.insertMany(records);
                }

                // cleanup old values
                if(existingRecs)
                {
                    let toDelete=[];
                    
                    for(let i = 0; i < existingRecs.length; i++)
                    {
                        let rec = existingRecs[i];

                        // delete old value here
                        if(!rec._keepIt)
                        {
                            let where = {}
                            where[this.fkey] = rec[this.fkey];
                            toDelete.push(where);
                        }
                    }

                    if(toDelete.length)
                        jointInst.deleteMany(toDelete);
                }
            }                
        } 
        catch (error) 
        {
            debug.error(error.message);
        }

        return doc_oid;        
    }
}

module.exports = new JointTable_decoratorSce();

// export types for jsdoc type checks
module.exports.JointTable_decoratorSce = JointTable_decoratorSce;
