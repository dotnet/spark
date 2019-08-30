using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    public sealed class Catalog : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;
        internal Catalog(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        ///<summary>
        ///Caches the specified table in-memory.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a table.
        /// If no database identifier is provided, it refers to a table in the current database.</param>
        public void CacheTable(string tableName) =>
            _jvmObject.Invoke("cacheTable", tableName);

        ///<summary>
        ///Removes all cached tables from the in-memory cache.
        ///</summary>
        public void ClearCache() => _jvmObject.Invoke("clearCache");

        ///<summary>
        ///Creates a table from the given path and returns the corresponding DataFrame.
        ///</summary>
        ///<param name="tableName">The name of the table to create</param>
        ///<param name="path">Path to use to create the table</param>
        ///<returns>DataFrame</returns>
        public DataFrame CreateTable(string tableName, string path) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("createTable", tableName, path));

        ///<summary>
        ///Creates a table from the given path based on a data source and returns the corresponding DataFrame.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a table.
        /// If no database identifier is provided, it refers to a table in the current database.</param>
        ///<param name="path">Path to use to create the table</param>
        ///<param name="source"></param>
        ///<returns>DataFrame</returns>
        public DataFrame CreateTable(string tableName, string path, string source) =>
            new DataFrame(
                (JvmObjectReference)_jvmObject.Invoke("createTable", tableName, path, source));

        ///<summary>
        ///Returns the current default database in this session.
        ///</summary>
        ///<returns>string</returns>
        public string CurrentDatabase() => (string)_jvmObject.Invoke("currentDatabase");

        ///<summary>
        ///Check if the database with the specified name exists.
        ///</summary>
        ///<param name="dbName">Name of the database to check</param>
        ///<returns>bool</returns>
        public bool DatabaseExists(string dbName) => 
            (bool)_jvmObject.Invoke("databaseExists", dbName);

        ///<summary>
        ///Drops the global temporary view with the given view name in the catalog.
        ///</summary>
        ///<param name="viewName">The unqualified name of the temporary view to be dropped.</param>
        ///<returns>bool</returns>
        public bool DropGlobalTempView(string viewName) =>
            (bool)_jvmObject.Invoke("dropGlobalTempView", viewName);

        ///<summary>
        ///Drops the local temporary view with the given view name in the catalog.
        /// Local temporary view is session-scoped. Its lifetime is the lifetime of the session
        /// that created it, i.e. it will be automatically dropped when the session terminates.
        /// It's not tied to any databases, i.e. we can't use db1.view1 to reference a local temporary view.
        ///</summary>
        ///<param name="viewName">The unqualified name of the temporary view to be dropped.</param>
        ///<returns>bool</returns>
        public bool DropTempView(string viewName) => 
            (bool)_jvmObject.Invoke("dropTempView", viewName);

        ///<summary>
        ///Check if the function with the specified name exists.
        ///</summary>
        ///<param name="functionName">Is either a qualified or unqualified name that designates a function.
        /// If no database identifier is provided, it refers to a function in the current database.</param>
        ///<returns>bool</returns>
        public bool FunctionExists(string functionName) =>
            (bool)_jvmObject.Invoke("functionExists", functionName);

        ///<summary>
        ///Check if the function with the specified name exists in the specified database.
        ///</summary>
        ///<param name="dbName">Is a name that designates a database.</param>
        ///<param name="functionName">Is an unqualified name that designates a function.</param>
        ///<returns>bool</returns>
        public bool FunctionExists(string dbName, string functionName) =>
            (bool)_jvmObject.Invoke("functionExists", dbName, functionName);

        ///<summary>
        ///Get the database with the specified name.
        ///</summary>
        ///<param name="dbName">Name of the database to get</param>
        ///<returns>Database</returns>
        public Database GetDatabase(string dbName) =>
            new Database((JvmObjectReference)_jvmObject.Invoke("getDatabase", dbName));

        ///<summary>
        ///Get the function with the specified name.
        ///</summary>
        ///<param name="functionName">Is either a qualified or unqualified name that designates a
        /// function. If no database identifier is provided, it refers to a temporary function or
        /// a function in the current database.</param>
        ///<returns>Function</returns>
        public Function GetFunction(string functionName) =>
            new Function((JvmObjectReference)_jvmObject.Invoke("getFunction", functionName));

        ///<summary>
        ///Get the function with the specified name.
        ///</summary>
        ///<param name="dbName">Is a name that designates a database.</param>
        ///<param name="functionName">Is an unqualified name that designates a function in the specified database.</param>
        ///<returns>Function</returns>
        public Function GetFunction(string dbName, string functionName) =>
            new Function((JvmObjectReference)_jvmObject.Invoke("getFunction", dbName, functionName));

        ///<summary>
        ///Get the table or view with the specified name.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a table. If no database identifier is provided, it refers to a table in the current database.</param>
        ///<returns>Table</returns>
        public Table GetTable(string tableName) =>
            new Table((JvmObjectReference)_jvmObject.Invoke("getTable", tableName));
        
        ///<summary>
        ///Get the table or view with the specified name in the specified database.
        ///</summary>
        ///<param name="dbName">Is a name that designates a database.</param>
        ///<param name="tableName">Is an unqualified name that designates a table in the specified database.</param>
        ///<returns>Table</returns>
        public Table GetTable(string dbName, string tableName) =>
            new Table((JvmObjectReference)_jvmObject.Invoke("getTable", dbName, tableName));
        
        ///<summary>
        ///Returns true if the table is currently cached in-memory.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a table. If no database identifier is provided, it refers to a table in the current database.</param>
        ///<returns>bool</returns>
        public bool IsCached(string tableName) =>
            (bool)_jvmObject.Invoke("isCached", tableName);


        ///<summary>
        ///Returns a list of columns for the given table/view or temporary view.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a table. If no database identifier is provided, it refers to a table in the current database.</param>
        ///<returns>DataFrame</returns>
        public DataFrame ListColumns(string tableName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listColumns", tableName));

        ///<summary>
        ///Returns a list of columns for the given table/view in the specified database.
        ///</summary>
        ///<param name="dbName">Is a name that designates a database.</param>
        ///<param name="tableName">Is an unqualified name that designates a table in the specified database.</param>
        ///<returns>DataFrame</returns>
        public DataFrame ListColumns(string dbName, string tableName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listColumns", dbName, tableName));

        ///<summary>
        ///Returns a list of databases available across all sessions.
        ///</summary>
        ///<returns>DataFrame</returns>
        public DataFrame ListDatabases() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listDatabases"));

        ///<summary>
        ///Returns a list of functions registered in the current database.
        ///</summary>
        ///<returns>DataFrame</returns>
        public DataFrame ListFunctions() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listFunctions"));

        ///<summary>
        ///Returns a list of functions registered in the specified database.
        ///</summary>
        ///<param name="dbName">Is a name that designates a database.</param>
        ///<returns>DataFrame</returns>
        public DataFrame ListFunctions(string dbName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listFunctions", dbName));

        ///<summary>
        ///Returns a list of tables/views in the current database.
        ///</summary>
        ///<returns>DataFrame</returns>
        public DataFrame ListTables() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listTables"));

        ///<summary>
        ///Returns a list of tables/views in the specified database.
        ///</summary>
        ///<param name="dbName">Is a name that designates a database.</param>
        ///<returns>DataFrame</returns>
        public DataFrame ListTables(string dbName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listTables", dbName));

        ///<summary>
        ///Recovers all the partitions in the directory of a table and update the catalog.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        public void RecoverPartitions(string tableName) =>
            _jvmObject.Invoke("recoverPartitions", tableName);

        ///<summary>
        ///Invalidates and refreshes all the cached data (and the associated metadata) for any
        /// Dataset that contains the given data source path. Path matching is by prefix,
        /// i.e. "/" would invalidate everything that is cached.
        ///</summary>
        ///<param name="path">Path to refresh</param>
        public void RefreshByPath(string path) => _jvmObject.Invoke("refreshByPath", path);
        
        ///<summary>
        ///Invalidates and refreshes all the cached data and metadata of the given table.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a
        ///  table. If no database identifier is provided, it refers to a table in the current
        ///  database.</param>
        public void RefreshTable(string tableName) => _jvmObject.Invoke("refreshTable", tableName);
        
        ///<summary>
        ///Sets the current default database in this session.
        ///</summary>
        ///<param name="dbName">The name of the database to set.</param>
        public void SetCurrentDatabase(string dbName) =>
            _jvmObject.Invoke("setCurrentDatabase", dbName);

        ///<summary>
        ///Check if the table or view with the specified name exists.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        ///<returns>bool</returns>
        public bool TableExists(string tableName) =>
            (bool)_jvmObject.Invoke("tableExists", tableName);

        ///<summary>
        ///Check if the table or view with the specified name exists in the specified database.
        ///</summary>
        ///<param name="dbName">Is a name that designates a database.</param>
        ///<param name="tableName">Is an unqualified name that designates a table.</param>
        /// <returns>bool</returns>
        public bool TableExists(string dbName, string tableName) =>
            (bool)_jvmObject.Invoke("tableExists", dbName, tableName);


        ///<summary>
        ///Removes the specified table from the in-memory cache.
        ///</summary>
        ///<param name="tableName">is either a qualified or unqualified name that designates a table. If no database identifier is provided, it refers to a table in the current database.</param>
        public void UncacheTable(string tableName) => _jvmObject.Invoke("uncacheTable", tableName);

    }
}
