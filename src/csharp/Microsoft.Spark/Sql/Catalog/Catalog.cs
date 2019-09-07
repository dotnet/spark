// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql.Catalog
{
    /// <summary>
    /// Catalog interface for Spark. To access this, use SparkSession.Catalog.
    /// </summary>
    public sealed class Catalog : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Catalog(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Caches the specified table in-memory.
        ///
        /// Spark SQL can cache tables using an in-memory columnar format by calling
        /// CacheTable("tableName") or DataFrame.Cache(). Then Spark SQL will scan only required
        /// columns and will automatically tune compression to minimize memory usage and GC
        /// pressure. You can call UncacheTable("tableName") to remove the table from memory.
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        public void CacheTable(string tableName) => _jvmObject.Invoke("cacheTable", tableName);

        /// <summary>
        /// Removes all cached tables from the in-memory cache. You can either clear all cached
        /// tables at once using this or clear each table individually using
        /// UncacheTable("tableName")
        /// </summary>
        public void ClearCache() => _jvmObject.Invoke("clearCache");

        /// <summary>
        /// Creates a table, in the hive warehouse, from the given path and returns the
        /// corresponding DataFrame. The table will contains the contents of the parquet
        /// file that is in the "path" parameter
        /// </summary>
        /// <param name="tableName">The name of the table to create</param>
        /// <param name="path">Path to use to create the table</param>
        /// <returns>DataFrame</returns>
        public DataFrame CreateTable(string tableName, string path) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("createTable", tableName, path));

        /// <summary>
        /// Creates a table, in the hive warehouse, from the given path based from a data
        /// source and returns the corresponding DataFrame.
        ///
        /// This reads the contents from the table from the data source and stores it in the
        /// location provided by "path"
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        /// <param name="path">Path to use to create the table</param>
        /// <param name="source">Data source to use to create the table</param>
        /// <returns>DataFrame</returns>
        public DataFrame CreateTable(string tableName, string path, string source) =>
            new DataFrame(
                (JvmObjectReference)_jvmObject.Invoke("createTable", tableName, path, source));

        /// <summary>
        /// Returns the current database in this session. By default your session will be
        /// connected to the "default" database (named "default") and to change database
        /// either use:
        ///         SetCurrentDatabase("databaseName")
        ///     or
        ///         SparkSession.Sql("USE DATABASE databaseName")
        /// </summary>
        /// <returns>string</returns>
        public string CurrentDatabase() => (string)_jvmObject.Invoke("currentDatabase");

        /// <summary>
        /// Check if the database with the specified name exists. This will check the list
        /// of hive databases in the current session to see if the database exists.
        ///
        /// If you use the internal hive catalog then you will have a "default" database.
        ///
        /// You can create new databases using Spark.Sql("CREATE DATABASE NAME");
        /// </summary>
        /// <param name="dbName">Name of the database to check</param>
        /// <returns>bool</returns>
        public bool DatabaseExists(string dbName) =>
            (bool)_jvmObject.Invoke("databaseExists", dbName);

        /// <summary>
        /// Drops the global temporary view with the given view name in the catalog.
        ///
        /// You can create global temporary views by taking a DataFrame and calling
        /// DataFrame.CreateOrReplaceGlobalTempView
        /// </summary>
        /// <param name="viewName">The unqualified name of the temporary view to be dropped.
        /// </param>
        /// <returns>bool</returns>
        public bool DropGlobalTempView(string viewName) =>
            (bool)_jvmObject.Invoke("dropGlobalTempView", viewName);

        /// <summary>
        /// Drops the local temporary view with the given view name in the catalog.
        /// Local temporary view is session-scoped. Its lifetime is the lifetime of the session
        /// that created it, i.e. it will be automatically dropped when the session terminates.
        /// It's not tied to any databases, i.e. we can't use db1.view1 to reference a local
        /// temporary view.
        ///
        /// You can create temporary views by taking a DataFrame and calling
        /// DataFrame.CreateOrReplaceTempView
        /// </summary>
        /// <param name="viewName">The unqualified name of the temporary view to be dropped.
        /// </param>
        /// <returns>bool</returns>
        public bool DropTempView(string viewName) => 
            (bool)_jvmObject.Invoke("dropTempView", viewName);

        /// <summary>
        /// Check if the function with the specified name exists. FunctionsExists includes in-built
        /// functions such as abs. To see if a built-in function exists you must use the
        /// unqualified name. If you create a function you can use the qualified name.
        /// </summary>
        /// <param name="functionName">Is either a qualified or unqualified name that designates a
        /// function. If no database identifier is provided, it refers to a function in the
        /// current database.</param>
        /// <returns>bool</returns>
        public bool FunctionExists(string functionName) =>
            (bool)_jvmObject.Invoke("functionExists", functionName);

        /// <summary>
        /// Check if the function with the specified name exists in the specified database. If you
        /// want to check if a built-in function exists specify the dbName as null or use
        /// FunctionExists(functionName)
        /// </summary>
        /// <param name="dbName">Is a name that designates a database.</param>
        /// <param name="functionName">Is an unqualified name that designates a function.</param>
        /// <returns>bool</returns>
        public bool FunctionExists(string dbName, string functionName) =>
            (bool)_jvmObject.Invoke("functionExists", dbName, functionName);

        /// <summary>
        /// Get the database with the specified name.
        ///
        /// Calling GetDatabase gives you access to the hive database name, description and
        /// location
        /// </summary>
        /// <param name="dbName">Name of the database to get</param>
        /// <returns>Database</returns>
        public Database GetDatabase(string dbName) =>
            new Database((JvmObjectReference)_jvmObject.Invoke("getDatabase", dbName));

        /// <summary>
        /// Get the function with the specified name. If you are trying to get an in-built
        /// function then use the unqualified name
        /// </summary>
        /// <param name="functionName">Is either a qualified or unqualified name that designates a
        /// function. If no database identifier is provided, it refers to a temporary function or
        /// a function in the current database.</param>
        /// <returns>Function</returns>
        public Function GetFunction(string functionName) =>
            new Function((JvmObjectReference)_jvmObject.Invoke("getFunction", functionName));

        /// <summary>
        /// Get the function with the specified name. If you are trying to get an in-built function
        /// then pass null as the dbName
        /// </summary>
        /// <param name="dbName">Is a name that designates a database. Built-in functions will be
        /// in database null rather than default</param>
        /// <param name="functionName">Is an unqualified name that designates a function in the
        /// specified database.</param>
        /// <returns>Function</returns>
        public Function GetFunction(string dbName, string functionName) =>
            new Function(
                (JvmObjectReference)_jvmObject.Invoke("getFunction", dbName, functionName));

        /// <summary>
        /// Get the table or view with the specified name. You can use this to find the tables
        /// description, database, type and whether it is a temporary table or not.
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        /// <returns>Table</returns>
        public Table GetTable(string tableName) =>
            new Table((JvmObjectReference)_jvmObject.Invoke("getTable", tableName));

        /// <summary>
        /// Get the table or view with the specified name in the specified database. You can use
        /// this to find the tables description, database, type and whether it is a temporary
        /// table or not
        /// </summary>
        /// <param name="dbName">Is a name that designates a database.</param>
        /// <param name="tableName">Is an unqualified name that designates a table in the specified
        /// database.</param>
        /// <returns>Table</returns>
        public Table GetTable(string dbName, string tableName) =>
            new Table((JvmObjectReference)_jvmObject.Invoke("getTable", dbName, tableName));
        
        /// <summary>
        /// Returns true if the table is currently cached in-memory. If the table is cached then it
        /// will consume memory. To remove the table from cache use UncacheTable or ClearCache
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        /// <returns>bool</returns>
        public bool IsCached(string tableName) => (bool)_jvmObject.Invoke("isCached", tableName);

        /// <summary>
        /// Returns a list of columns for the given table/view or temporary view. The DataFrame
        /// includes the name, description, dataType, whether it is nullable or if it is
        /// partitioned and if it is broken in buckets.
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        /// <returns>DataFrame</returns>
        public DataFrame ListColumns(string tableName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listColumns", tableName));

        /// <summary>
        /// Returns a list of columns for the given table/view in the specified database.
        /// The DataFrame includes the name, description, dataType, whether it is nullable or if it
        /// is partitioned and if it is broken in buckets 
        /// </summary>
        /// <param name="dbName">Is a name that designates a database.</param>
        /// <param name="tableName">Is an unqualified name that designates a table in the specified
        /// database.</param>
        /// <returns>DataFrame</returns>
        public DataFrame ListColumns(string dbName, string tableName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listColumns", dbName, tableName));

        /// <summary>
        /// Returns a list of databases available across all sessions. The DataFrame contains
        /// the name, description and locationUri of each database
        /// </summary>
        /// <returns>DataFrame</returns>
        public DataFrame ListDatabases() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listDatabases"));

        /// <summary>
        /// Returns a list of functions registered in the current database. This includes all
        /// temporary functions. The DataFrame contains the class name, database, description,
        /// whether it is temporary and the name of the function.
        /// </summary>
        /// <returns>DataFrame</returns>
        public DataFrame ListFunctions() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listFunctions"));

        /// <summary>
        /// Returns a list of functions registered in the specified database. This includes all
        /// temporary functions. The DataFrame contains the class name, database, description,
        /// whether it is temporary and the name of the function.
        /// </summary>
        /// <param name="dbName">Is a name that designates a database.</param>
        /// <returns>DataFrame</returns>
        public DataFrame ListFunctions(string dbName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listFunctions", dbName));

        /// <summary>
        /// Returns a list of tables/views in the current database. The DataFrame includes the
        /// name, database, description, table type and whether the table is temporary or not.
        /// </summary>
        /// <returns>DataFrame</returns>
        public DataFrame ListTables() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listTables"));

        /// <summary>
        /// Returns a list of tables/views in the specified database. The DataFrame includes the
        /// name, database, description, table type and whether the table is temporary or not.
        /// </summary>
        /// <param name="dbName">Is a name that designates a database.</param>
        /// <returns>DataFrame</returns>
        public DataFrame ListTables(string dbName) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("listTables", dbName));

        /// <summary>
        /// Recovers all the partitions in the directory of a table and update the catalog. This
        /// only works for partitioned tables and not un-partitioned tables or views.
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        public void RecoverPartitions(string tableName) =>
            _jvmObject.Invoke("recoverPartitions", tableName);

        /// <summary>
        /// Invalidates and refreshes all the cached data (and the associated metadata) for any
        /// Dataset that contains the given data source path. Path matching is by prefix,
        /// i.e. "/" would invalidate everything that is cached.
        /// </summary>
        /// <param name="path">Path to refresh</param>
        public void RefreshByPath(string path) => _jvmObject.Invoke("refreshByPath", path);

        /// <summary>
        /// Invalidates and refreshes all the cached data and metadata of the given table. For
        /// performance reasons, Spark SQL or the external data source library it uses might cache
        /// certain metadata about a table, such as the location of blocks. When those change
        /// outside of Spark SQL, users should call this function to invalidate the cache. If this
        /// table is cached as an InMemoryRelation, drop the original cached version and make the
        /// new version cached lazily.
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        public void RefreshTable(string tableName) => _jvmObject.Invoke("refreshTable", tableName);
        
        /// <summary>
        /// Sets the current default database in this session.
        /// </summary>
        /// <param name="dbName">The name of the database to set.</param>
        public void SetCurrentDatabase(string dbName) =>
            _jvmObject.Invoke("setCurrentDatabase", dbName);

        /// <summary>
        /// Check if the table or view with the specified name exists. This can either be a
        /// temporary view or a table/view.
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        /// <returns>bool</returns>
        public bool TableExists(string tableName) =>
            (bool)_jvmObject.Invoke("tableExists", tableName);

        /// <summary>
        /// Check if the table or view with the specified name exists in the specified database.
        /// </summary>
        /// <param name="dbName">Is a name that designates a database.</param>
        /// <param name="tableName">Is an unqualified name that designates a table.</param>
        /// <returns>bool</returns>
        public bool TableExists(string dbName, string tableName) => 
            (bool)_jvmObject.Invoke("tableExists", dbName, tableName);

        /// <summary>
        /// Removes the specified table from the in-memory cache.
        /// </summary>
        /// <param name="tableName">Is either a qualified or unqualified name that designates a
        /// table. If no database identifier is provided, it refers to a table in the current
        /// database.</param>
        public void UncacheTable(string tableName) => _jvmObject.Invoke("uncacheTable", tableName);
    }
}
