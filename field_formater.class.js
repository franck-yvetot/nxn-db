class FieldFormater 
{
    _formatRecord(rec,view) 
    {
        const format = view.getFieldsFormats();
        const locale = view.locale();
        const schema = view.schema();

        if(!format)
            return  Object.assign({}, rec);
        
        objectSce.forEachSync(format,(vcsv,k) => 
        {
            let av = vcsv.split(',');
            for (let i=av.length-1;i>=0;i--)
            {
                // execute in reverse order
                let v = av[i].trim();
                const func = "_format_"+v;
                const fdesc = schema.field(k);
                if(typeof this[func] == "function")
                    this[func](k,rec,fdesc,locale);
            }
        });

        return rec;
    }

    _format_json(fname,rec,fdesc,locale) 
    {
        if(rec[fname])
        {
            rec[fname] = JSON.parse(rec[fname]);
        }
    }

    _format_base64(fname,rec,fdesc,locale) 
    {
        if(rec[fname])
        {
            rec[fname] = Buffer.from(rec[fname], 'base64').toString('utf8');
        }
    }
    
    _format_enum(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        if(v && typeof (rec[fname+'__html']) != "undefined")
        {
            // support SQL
            rec[fname] = {
                value:v,
                html:rec[fname+'__html']
            };
            delete rec[fname+'__html'];
        }
        else if(v && v.html)
        {
            rec[fname] = v;
        }
        else
        {
            let html = v && (fdesc.getEnum && fdesc.getEnum(v,",",locale)) || '';
            rec[fname] = {
                value:v,
                html
            };
        }
    }

    _format_enum_reg(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        let html = v || "";

        if(html)
        {
            let format = fdesc._prop("x-enum-reg-format");
            if(format)
            {
                let pattern = format.reg;
                let regEx = new RegExp(pattern, "gm");
                let rep = format.html;
                html = v.replace(regEx,rep) || v || "";
            }    
        }

        rec[fname] = {
            value:v,
            html
        };
    }

    _format_enum_email_name(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        let html = v || "";

        if(html)
        {
            try {
                html = html
                .split("@")[0]
                .split(".")
                .map(mot => mot.charAt(0).toUpperCase() + mot.slice(1).toLowerCase())
                .join(' ');
            }
            catch(error)
            {
            }
        }

        rec[fname] = {
            value:v,
            html
        };
    }    

    _format_enum_upper_initial_html(fname,rec,fdesc,locale) 
    {
        let v = rec[fname];
        if(v.html)
            v.html = v.html
                .split(' ')
                .map(mot => mot.charAt(0).toUpperCase() + mot.slice(1).toLowerCase())
                .join(' ');
    }    

    _format_enum_static(fname,rec,fdesc,locale) 
    {
        if(rec[fname])
        {
            let v = rec[fname];
            let html;
            if(fdesc && fdesc.getEnum)
                html = fdesc.getEnum(v,",",locale);

            rec[fname] = {
                value:v,
                //html:(locale && locale.e_(rec[fname],fname)) || rec[fname]
                html
            };
        }
    }

    _formatLocaleValues(fname,rec) 
    {
        rec[fname] = this._locale.v_(rec[fname]);
    }        
}

module.exports = FieldFormater;