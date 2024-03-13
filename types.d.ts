declare module "nxn_db"
{
    interface TMySQLFindOptions 
    {
        limit;
        skip;
        view;
        collection;
        withMeta;
        withLocale;
    }

    interface TMySQLResult 
    {
        data: Record<string,any>
        metadata: Record<string,{fields:Record<string,any>}>        
    }

    type TMySQLRecord = Record<string,any>
}