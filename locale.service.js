const debug = require("@nxn/debug")('Locale');

// locale for a lang
class Lang_Locale
{
    constructor(lang=null,localeStrings=null) {
        const locale = localeStrings || {};
        this.lang = lang || 'en';

        this.fields = locale.fields || {};
        this.strings = locale.strings || {};
        this.enums = locale.enums || {};
        this.workflows = locale.workflows || {};
    }

    // get i8n string
    _(id) {
        return this.strings[id] || id;
    }

    // get i8n enum value
    e_(id,fname,deft=null) {
        // lang = lang || this.lang;
        return (this.enums[fname] && this.enums[fname][id]) || deft || id;
    }

    /** returns an enum set for a field */
    enumList(fname,deft) {
        // lang = lang || this.lang;
        return this.enums[fname] || deft;
    }

    // get i8n field label
    f_(id) {
        return this.fields[id] || (id.charAt(0).toUpperCase() + id.slice(1).replace(/_/g,' '));
    }

    // map workflow state def
    stateDef(workflowId,stateId,stateDef)
    {
        if(!this.workflows[workflowId] || !this.workflows[workflowId].states)
            return stateDef;
        
        if(this.workflows[workflowId].states[stateId])
            return {...stateDef, ...this.workflows[workflowId].states[stateId]};

        return stateDef;
    }
}

// set of locales (multi languages)
class Locale
{
    constructor() {
        this.config = {};
    }

    invalidParam(str) {
        debug.error(str);
        throw new Error(str);
    }

    init(config) {
        this.config = config;
        this.locales = {};

        if(config.langs)
        {
            for (let l in config.langs)
            {
                this.locales[l] = new Lang_Locale(l,config.langs[l]);
            }
        }

        this.lang = config.default || 'en';
        this.deftLocale = this.locales[this.lang] || new Lang_Locale(this.lang);
    }

    localeByLang(lang) {
        return (this.locales[lang||this.lang] || this.deftLocale);
    }

    // get i8n string
    _(id,lang=null) {
        return (this.locales[lang||this.lang] || this.deftLocale)._(id);
    }

    // get i8n enum value
    e_(id,fname,deft=null,lang=null) {
        return (this.locales[lang||this.lang] || this.deftLocale).e_(id,fname,deft);
    }

    /** returns an enum set for a field */
    enums(fname,deft=null,lang=null) {
        return (this.locales[lang||this.lang] || this.deftLocale).enumList(fname,deft);
    }

    // get i8n field label
    f_(id,lang=null) {
        return (this.locales[lang||this.lang] || this.deftLocale).f_(id);
    }

    stateDef(workflowId,stateId,stateDef,lang=null) {
        return (this.locales[lang||this.lang] || this.deftLocale).stateDef(workflowId,stateId,stateDef);
    }
}

class LocaleFactory
{
    constructor () {
        this.instances={};
    }
    getInstance(instName) {
        if(this.instances[instName])
            return this.instances[instName];

        return (this.instances[instName] = new Locale(instName));
    }
}

module.exports = new LocaleFactory();
