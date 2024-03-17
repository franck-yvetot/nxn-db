declare module "@nxn/db" {
    export interface TMySQLFindOptions 
    {
        limit;
        skip;
        view;
        collection;
        withMeta;
        withLocale;
    }

    /** mysl data res */
    export interface TMySQLResult 
    {
        data: Record<string,any>
        metadata: Record<string,{fields:Record<string,any>}>        
    }

    export type TMySQLRecord = Record<string,any>

    export interface IClientManager 
    {
        getClientIdByToken(gtoken: any): Promise<string>;
        getClientInfosById(client_id: string,section): Object | null;
        getClientIdByEmail(email: string): Promise<string>;
    }       
}