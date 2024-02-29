const debug = require("@nxn/debug")('File_reader');
const FlowNode = require("@nxn/boot/node");
const fs = require("@nxn/files");
const arraySce = require("@nxn/ext/array.service");
var path = require('path');

class SQL_exec_Node extends FlowNode
{
    constructor(inst) {
        super(inst);
    }

    async init(config,ctxt,...injections) {
        super.init(config,ctxt,injections);

        // get db
        this.db = this.getInjection("db");
        if(this.db)
        {
            this.conHandler = await this.db.connect();
            this.con = await this.conHandler.getCon();;
        }

        // output node for tracing
        this.traceNodes = this.getInjections("log") || null;
        this.traceErrorNodes = this.getInjections("errors") || null;
    }

    async processMessage(message) {

        try {
            const {name,data,path} = message;

            await this.executeFile(path,data,name);
        } 
        catch(error) {
            let message = error.message || error;
            let code = parseInt(error.code||500, 10) || 500;
            debug.error(error.stack||error);
        }
    }

    async executeFile(fpath,content,name) {
        console.log("Execute file "+fpath);
        if(!content.split)
            content = content.toString('utf8');

        const sqlStatements = content.split(';');

        for (let statement of sqlStatements) {
            statement = this.removeSQLComments(statement);
            if(statement)
                await this.executeLine(statement,name);
        }
    }

    removeSQLComments(sqlString) {
        // Matches /*...*/ or -- ... comments and removes them
        return sqlString.replace(/(\/\*[\s\S]*?\*\/|--.*)/g, '').replace(/[\n\r]+/g," ").trim();
      }    

    async executeLine(line,name) 
    {
        try 
        {
            await this.db.query(line,null,null,false,null,this.con) ;
            this.trace("\n\nSUCCESS SQL : \n"+line, name);
        } 
        catch (error) 
        {
            this.traceError("\n\nERROR SQL : \n"+line+ "\nerror:"+error.message+ " "+error.stack, name);
        }
    }

    async trace(line,name) 
    {
        if(this.traceNodes)
        {
            this.sendMessage({name: name+".log",data:line},this.traceNodes);            
        }
    }

    async traceError(line,name) 
    {
        // trace errors
        if(this.traceErrorNodes)
        {
            this.sendMessage({name: name+".log",data:line},this.traceErrorNodes);            
        }

        // add it to log as well
        this.trace(line,name);
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

        return (this.instances[instName] = new SQL_exec_Node(instName));
    }
}

module.exports = new Factory();