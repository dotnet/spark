// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Expressions;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Column class represents a column that will be computed based on the data in a DataFrame.
    /// </summary>
    public sealed class Column : IJvmObjectReferenceProvider
    {

        /// <summary>
        /// Constructor for Column class.
        /// </summary>
        /// <param name="jvmObject">JVM object reference</param>
        internal Column(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Negate the given column.
        /// </summary>
        /// <param name="self">Column to negate</param>
        /// <returns>New column after applying negation</returns>
        public static Column operator -(Column self)
        {
            return ApplyFunction(self, "negate");
        }

        /// <summary>
        /// Apply inversion of boolean expression, i.e. NOT.
        /// </summary>
        /// <param name="self">Column to apply inversion</param>
        /// <returns>New column after applying inversion</returns>
        public static Column operator !(Column self)
        {
            return ApplyFunction(self, "not");
        }

        /// <summary>
        /// Apply equality test on the given two columns.
        /// </summary>
        /// <param name="lhs">Column on the left side of equality test</param>
        /// <param name="rhs">Column on the right side of equality test</param>
        /// <returns>New column after applying equality test</returns>
        public static Column operator ==(Column lhs, object rhs)
        {
            return lhs.EqualTo(rhs);
        }

        /// <summary>
        /// Equality test.
        /// </summary>
        /// <param name="rhs">The right hand side of expression being tested for equality</param>
        /// <returns>New column after applying the equal to operator</returns>
        public Column EqualTo(object rhs)
        {
            return ApplyMethod("equalTo", rhs);
        }

        /// <summary>
        /// Apply inequality test.
        /// </summary>
        /// <param name="lhs">Column on the left side of inequality test</param>
        /// <param name="rhs">Column on the right side of inequality test</param>
        /// <returns>New column after applying inequality test</returns>
        public static Column operator !=(Column lhs, object rhs)
        {
            return lhs.NotEqual(rhs);
        }

        /// <summary>
        /// Inequality test.
        /// </summary>
        /// <param name="rhs">
        /// The right hand side of expression being tested for inequality.
        /// </param>
        /// <returns>New column after applying not equal operator</returns>
        public Column NotEqual(object rhs)
        {
            return ApplyMethod("notEqual", rhs);
        }

        /// <summary>
        /// Apply "greater than" operator for the given two columns.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Column on the right side of the operator</param>
        /// <returns>New column after applying the operator</returns>
        public static Column operator >(Column lhs, object rhs)
        {
            return lhs.Gt(rhs);
        }

        /// <summary>
        /// Greater than.
        /// </summary>
        /// <param name="rhs">
        /// The object that is in comparison to test if the left hand side is greater.
        /// </param>
        /// <returns>New column after applying the greater than operator</returns>
        public Column Gt(object rhs)
        {
            return ApplyMethod("gt", rhs);
        }

        /// <summary>
        /// Apply "less than" operator for the given two columns.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Column on the right side of the operator</param>
        /// <returns>New column after applying the operator</returns>
        public static Column operator <(Column lhs, object rhs)
        {
            return lhs.Lt(rhs);
        }

        /// <summary>
        /// Less than.
        /// </summary>
        /// <param name="rhs">
        /// The object that is in comparison to test if the left hand side is lesser.
        /// </param>
        /// <returns>New column after applying the less than operator</returns>
        public Column Lt(object rhs)
        {
            return ApplyMethod("lt", rhs);
        }

        /// <summary>
        /// Apply "less than or equal to" operator for the given two columns.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Column on the right side of the operator</param>
        /// <returns>New column after applying the operator</returns>
        public static Column operator <=(Column lhs, object rhs)
        {
            return lhs.Leq(rhs);
        }

        /// <summary>
        /// Less than or equal to.
        /// </summary>
        /// <param name="rhs">
        /// The object that is in comparison to test if the left hand side is less or equal to.
        /// </param>
        /// <returns>New column after applying the less than or equal to operator</returns>
        public Column Leq(object rhs)
        {
            return ApplyMethod("leq", rhs);
        }

        /// <summary>
        /// Apply "greater than or equal to" operator for the given two columns.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Column on the right side of the operator</param>
        /// <returns>New column after applying the operator</returns>
        public static Column operator >=(Column lhs, object rhs)
        {
            return lhs.Geq(rhs);
        }

        /// <summary>
        /// Greater or equal to.
        /// </summary>
        /// <param name="rhs">
        /// The object that is in comparison to test if the left hand side is greater or equal to
        /// </param>
        /// <returns>New column after applying the greater or equal to operator</returns>
        public Column Geq(object rhs)
        {
            return ApplyMethod("geq", rhs);
        }

        /// <summary>
        /// Apply equality test that is safe for null values.
        /// </summary>
        /// <param name="obj">Object to apply equality test</param>
        /// <returns>New column after applying the equality test</returns>
        public Column EqNullSafe(object obj)
        {
            return ApplyMethod("eqNullSafe", obj);
        }

        /// <summary>
        /// Evaluates a condition and returns one of multiple possible result expressions.
        /// If Otherwise(object) is not defined at the end, null is returned for
        /// unmatched conditions. This method can be chained with other 'when' invocations in case
        /// multiple matches are required.
        /// </summary>
        /// <param name="condition">The condition to check</param>
        /// <param name="value">The value to set if the condition is true</param>
        /// <returns>New column after applying the when method</returns>
        public Column When(Column condition, object value)
        {
            return ApplyMethod("when", condition, value);
        }

        /// <summary>
        /// Evaluates a list of conditions and returns one of multiple possible result expressions.
        /// If otherwise is not defined at the end, null is returned for unmatched conditions.
        /// This is used when the When(Column, object) method is applied.
        /// </summary>
        /// <param name="value">The value to set</param>
        /// <returns>New column after applying otherwise method</returns>
        public Column Otherwise(object value)
        {
            return ApplyMethod("otherwise", value);
        }

        /// <summary>
        /// True if the current column is between the lower bound and upper bound, inclusive.
        /// </summary>
        /// <param name="lowerBound">The lower bound</param>
        /// <param name="upperBound">The upper bound</param>
        /// <returns>New column after applying the between method</returns>
        public Column Between(object lowerBound, object upperBound)
        {
            return ApplyMethod("between", lowerBound, upperBound);
        }

        /// <summary>
        /// True if the current expression is NaN.
        /// </summary>
        /// <returns>
        /// New column with values true if the preceding column had a NaN
        /// value in the same index, and false otherwise.
        /// </returns>
        public Column IsNaN()
        {
            return ApplyMethod("isNaN");
        }

        /// <summary>
        /// True if the current expression is null.
        /// </summary>
        /// <returns>
        /// New column with values true if the preceding column had a null
        /// value in the same index, and false otherwise.
        /// </returns>
        public Column IsNull()
        {
            return ApplyMethod("isNull");
        }

        /// <summary>
        /// True if the current expression is NOT null.
        /// </summary>
        /// <returns>
        /// New column with values true if the preceding column had a non-null
        /// value in the same index, and false otherwise.
        /// </returns>
        public Column IsNotNull()
        {
            return ApplyMethod("isNotNull");
        }

        /// <summary>
        /// Apply boolean OR operator for the given two columns.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Column on the right side of the operator</param>
        /// <returns>New column after applying the operator</returns>
        public static Column operator |(Column lhs, Column rhs)
        {
            // Check the comment for operator & why rhs is Column instead of object.
            return lhs.Or(rhs);
        }

        /// <summary>
        /// Apply boolean OR operator with the given column.
        /// </summary>
        /// <param name="other">Column to apply OR operator</param>
        /// <returns>New column after applying the operator</returns>
        public Column Or(Column other)
        {
            return ApplyMethod("or", other);
        }

        /// <summary>
        /// Apply boolean AND operator for the given two columns.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Column on the right side of the operator</param>
        /// <returns>New column after applying the operator</returns>
        public static Column operator &(Column lhs, Column rhs)
        {
            // Note that in Spark, && is overloaded which takes "Any" for the rhs.
            // Since the overloaded operator on JVM cannot be reflected/called,
            // this is calling "and" instead, which takes in "Column" for the rhs.
            return lhs.And(rhs);
        }

        /// <summary>
        /// Apply boolean AND operator with the given column.
        /// </summary>
        /// <param name="other">Column to apply AND operator</param>
        /// <returns>New column after applying the operator</returns>
        public Column And(Column other)
        {
            return ApplyMethod("and", other);
        }

        /// <summary>
        /// Apply sum of two expressions.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Object on the right side of the operator</param>
        /// <returns>New column after applying the sum operation</returns>
        public static Column operator +(Column lhs, object rhs)
        {
            return lhs.Plus(rhs);
        }

        /// <summary>
        /// Sum of this expression and another expression.
        /// </summary>
        /// <param name="rhs">The expression to be summed with</param>
        /// <returns>New column after applying the plus operator</returns>
        public Column Plus(object rhs)
        {
            return ApplyMethod("plus", rhs);
        }

        /// <summary>
        /// Apply subtraction of two expressions.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Object on the right side of the operator</param>
        /// <returns>New column after applying the subtraction operation</returns>
        public static Column operator -(Column lhs, object rhs)
        {
            return lhs.Minus(rhs);
        }

        /// <summary>
        /// Subtraction. Subtract the other expression from this expression.
        /// </summary>
        /// <param name="rhs">The expression to be subtracted with</param>
        /// <returns>New column after applying the minus operator</returns>
        public Column Minus(object rhs)
        {
            return ApplyMethod("minus", rhs);
        }

        /// <summary>
        /// Apply multiplication of two expressions.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Object on the right side of the operator</param>
        /// <returns>New column after applying the multiplication operation</returns>
        public static Column operator *(Column lhs, object rhs)
        {
            return lhs.Multiply(rhs);
        }

        /// <summary>
        /// Multiplication of this expression and another expression.
        /// </summary>
        /// <param name="rhs">The expression to be multiplied with</param>
        /// <returns>New column after applying the multiply operator</returns>
        public Column Multiply(object rhs)
        {
            return ApplyMethod("multiply", rhs);
        }

        /// <summary>
        /// Apply division of two expressions.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Object on the right side of the operator</param>
        /// <returns>New column after applying the division operation</returns>
        public static Column operator /(Column lhs, object rhs)
        {
            return lhs.Divide(rhs);
        }

        /// <summary>
        /// Division of this expression by another expression.
        /// </summary>
        /// <param name="rhs">The expression to be divided by</param>
        /// <returns>New column after applying the divide operator</returns>
        public Column Divide(object rhs)
        {
            return ApplyMethod("divide", rhs);
        }

        /// <summary>
        /// Apply division of two expressions.
        /// </summary>
        /// <param name="lhs">Column on the left side of the operator</param>
        /// <param name="rhs">Object on the right side of the operator</param>
        /// <returns>New column after applying the division operation</returns>
        public static Column operator %(Column lhs, object rhs)
        {
            return lhs.Mod(rhs);
        }

        /// <summary>
        /// Modulo (a.k.a remainder) expression.
        /// </summary>
        /// <param name="rhs">
        /// The expression to be divided by to get the remainder for.
        /// </param>
        /// <returns>New column after applying the mod operator</returns>
        public Column Mod(object rhs)
        {
            return ApplyMethod("mod", rhs);
        }

        /// <summary>
        /// SQL like expression. Returns a boolean column based on a SQL LIKE match.
        /// </summary>
        /// <param name="literal">The literal that is used to compute the SQL LIKE match</param>
        /// <returns>New column after applying the SQL LIKE match</returns>
        public Column Like(string literal)
        {
            return ApplyMethod("like", literal);
        }

        /// <summary>
        /// SQL RLIKE expression (LIKE with Regex). Returns a boolean column based on a regex
        /// match.
        /// </summary>
        /// <param name="literal">The literal that is used to compute the Regex match</param>
        /// <returns>New column after applying the regex LIKE method</returns>
        public Column RLike(string literal)
        {
            return ApplyMethod("rlike", literal);
        }

        /// <summary>
        /// An expression that gets an item at position `ordinal` out of an array,
        /// or gets a value by key `key` in a `MapType`.
        /// </summary>
        /// <param name="key">The key with which to identify the item</param>
        /// <returns>New column after getting an item given a specific key</returns>
        public Column GetItem(object key)
        {
            return ApplyMethod("getItem", key);
        }

        /// <summary>
        /// An expression that adds/replaces field in <see cref="Types.StructType"/> by name.
        /// </summary>
        /// <param name="fieldName">The name of the field</param>
        /// <param name="column">Column to assign to the field</param>
        /// <returns>
        /// New column after adding/replacing field in <see cref="Types.StructType"/> by name.
        /// </returns>
        [Since(Versions.V3_1_0)]
        public Column WithField(string fieldName, Column column)
        {
            return ApplyMethod("withField", fieldName, column);
        }

        /// <summary>
        /// An expression that drops fields in <see cref="Types.StructType"/> by name.
        /// </summary>
        /// <param name="fieldNames">Name of fields to drop.</param>
        /// <returns>New column after after dropping fields.</returns>
        [Since(Versions.V3_1_0)]
        public Column DropFields(params string[] fieldNames)
        {
            return ApplyMethod("dropFields", fieldNames);
        }

        /// <summary>
        /// An expression that gets a field by name in a `StructType`.
        /// </summary>
        /// <param name="fieldName">The name of the field</param>
        /// <returns>New column after getting a field for a specific key</returns>
        public Column GetField(string fieldName)
        {
            return ApplyMethod("getField", fieldName);
        }

        /// <summary>
        /// An expression that returns a substring.
        /// </summary>
        /// <param name="startPos">Expression for the starting position</param>
        /// <param name="len">Expression for the length of the substring</param>
        /// <returns>
        /// New column that is bound by the start position provided, and the length.
        /// </returns>
        public Column SubStr(Column startPos, Column len)
        {
            return ApplyMethod("substr", startPos, len);
        }

        /// <summary>
        /// An expression that returns a substring.
        /// </summary>
        /// <param name="startPos">Starting position</param>
        /// <param name="len">Length of the substring</param>
        /// <returns>
        /// New column that is bound by the start position provided, and the length.
        /// </returns>
        public Column SubStr(int startPos, int len)
        {
            return ApplyMethod("substr", startPos, len);
        }

        /// <summary>
        /// Contains the other element. Returns a boolean column based on a string match.
        /// </summary>
        /// <param name="other">
        /// The object that is used to check for existence in the current column.
        /// </param>
        /// <returns>New column after checking if the column contains object other</returns>
        public Column Contains(object other)
        {
            return ApplyMethod("contains", other);
        }

        /// <summary>
        /// String starts with. Returns a boolean column based on a string match.
        /// </summary>
        /// <param name="other">
        /// The other column containing strings with which to check how values
        /// in this column starts.
        /// </param>
        /// <returns>
        /// A boolean column where entries are true if values in the current
        /// column does indeed start with the values in the given column.
        /// </returns>
        public Column StartsWith(Column other)
        {
            return ApplyMethod("startsWith", other);
        }

        /// <summary>
        /// String starts with another string literal.
        /// Returns a boolean column based on a string match.
        /// </summary>
        /// <param name="literal">
        /// The string literal used to check how values in a column starts.
        /// </param>
        /// <returns>
        /// A boolean column where entries are true if values in the current column
        /// does indeed start with the given string literal.
        /// </returns>
        public Column StartsWith(string literal)
        {
            return ApplyMethod("startsWith", literal);
        }

        /// <summary>
        /// String ends with. Returns a boolean column based on a string match.
        /// </summary>
        /// <param name="other">
        /// The other column containing strings with which to check how values
        /// in this column ends.
        /// </param>
        /// <returns>
        /// A boolean column where entries are true if values in the current
        /// column does indeed end with the values in the given column.
        /// </returns>
        public Column EndsWith(Column other)
        {
            return ApplyMethod("endsWith", other);
        }

        /// <summary>
        /// String ends with another string literal. Returns a boolean column based
        /// on a string match.
        /// </summary>
        /// <param name="literal">
        /// The string literal used to check how values in a column ends.
        /// </param>
        /// <returns>
        /// A boolean column where entries are true if values in the current column
        /// does indeed end with the given string literal.
        /// </returns>
        public Column EndsWith(string literal)
        {
            return ApplyMethod("endsWith", literal);
        }

        /// <summary>
        /// Gives the column an alias. Same as `As()`.
        /// </summary>
        /// <param name="alias">The alias that is given</param>
        /// <returns>New column after applying an alias</returns>
        public Column Alias(string alias)
        {
            return ApplyMethod("alias", alias);
        }

        /// <summary>
        /// Gives the column an alias.
        /// </summary>
        /// <param name="alias">The alias that is given</param>
        /// <returns>New column after applying the as alias operator</returns>
        public Column As(string alias)
        {
            return Alias(alias);
        }

        /// <summary>
        /// Assigns the given aliases to the results of a table generating function.
        /// </summary>
        /// <param name="alias">A list of aliases</param>
        /// <returns>Column object</returns>
        public Column As(IEnumerable<string> alias)
        {
            return ApplyMethod("as", alias);
        }

        /// <summary>
        /// Extracts a value or values from a complex type.
        /// The following types of extraction are supported:
        ///
        /// 1. Given an Array, an integer ordinal can be used to retrieve a single value.
        /// 2. Given a Map, a key of the correct type can be used to retrieve an individual value.
        /// 3. Given a Struct, a string fieldName can be used to extract that field.
        /// 4. Given an Array of Structs, a string fieldName can be used to extract field
        /// of every struct in that array, and return an Array of fields.
        ///
        /// </summary>
        /// <param name="extraction">Object used to extract value(s) from the column</param>
        /// <returns>Column object</returns>
        public Column Apply(object extraction)
        {
            return ApplyMethod("apply", extraction);
        }

        /// <summary>
        /// Gives the column a name (alias).
        /// </summary>
        /// <param name="alias">Alias column name</param>
        /// <returns>Column object</returns>
        public Column Name(string alias)
        {
            return ApplyMethod("name", alias);
        }

        /// <summary>
        /// Casts the column to a different data type, using the canonical string
        /// representation of the type.
        /// </summary>
        /// <remarks>
        /// The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
        /// `float`, `double`, `decimal`, `date`, `timestamp`.
        /// </remarks>
        /// <param name="to">String version of datatype</param>
        /// <returns>Column object</returns>
        public Column Cast(string to)
        {
            return ApplyMethod("cast", to);
        }

        /// <summary>
        /// Returns a sort expression based on ascending order of the column,
        /// and null values return before non-null values.
        /// </summary>
        /// <returns>New column after applying the descending order operator</returns>
        public Column Desc()
        {
            return ApplyMethod("desc");
        }

        /// <summary>
        /// Returns a sort expression based on the descending order of the column,
        /// and null values appear before non-null values.
        /// </summary>
        /// <returns>Column object</returns>
        public Column DescNullsFirst()
        {
            return ApplyMethod("desc_nulls_first");
        }

        /// <summary>
        /// Returns a sort expression based on the descending order of the column,
        /// and null values appear after non-null values.
        /// </summary>
        /// <returns>Column object</returns>
        public Column DescNullsLast()
        {
            return ApplyMethod("desc_nulls_last");
        }

        /// <summary>
        /// Returns a sort expression based on ascending order of the column.
        /// </summary>
        /// <returns>New column after applying the ascending order operator</returns>
        public Column Asc()
        {
            return ApplyMethod("asc");
        }

        /// <summary>
        /// Returns a sort expression based on ascending order of the column,
        /// and null values return before non-null values.
        /// </summary>
        /// <returns></returns>
        public Column AscNullsFirst()
        {
            return ApplyMethod("asc_nulls_first");
        }

        /// <summary>
        /// Returns a sort expression based on ascending order of the column,
        /// and null values appear after non-null values.
        /// </summary>
        /// <returns></returns>
        public Column AscNullsLast()
        {
            return ApplyMethod("asc_nulls_last");
        }

        /// <summary>
        /// Prints the expression to the console for debugging purposes.
        /// </summary>
        /// <param name="extended">To print extended version or not</param>
        public void Explain(bool extended)
        {
            ApplyMethod("explain", extended);
        }

        /// <summary>
        /// Compute bitwise OR of this expression with another expression.
        /// </summary>
        /// <param name="other">
        /// The other column that will be used to compute the bitwise OR.
        /// </param>
        /// <returns>New column after applying bitwise OR operator</returns>
        public Column BitwiseOR(object other)
        {
            return ApplyMethod("bitwiseOR", other);
        }

        /// <summary>
        /// Compute bitwise AND of this expression with another expression.
        /// </summary>
        /// <param name="other">
        /// The other column that will be used to compute the bitwise AND.
        /// </param>
        /// <returns>New column after applying the bitwise AND operator</returns>
        public Column BitwiseAND(object other)
        {
            return ApplyMethod("bitwiseAND", other);
        }

        /// <summary>
        /// Compute bitwise XOR of this expression with another expression.
        /// </summary>
        /// <param name="other">
        /// The other column that will be used to compute the bitwise XOR.
        /// </param>
        /// <returns>New column after applying bitwise XOR operator</returns>
        public Column BitwiseXOR(object other)
        {
            return ApplyMethod("bitwiseXOR", other);
        }

        /// <summary>
        /// Defines a windowing column.
        /// </summary>
        /// <param name="window">
        /// A window specification that defines the partitioning, ordering, and frame boundaries.
        /// </param>
        /// <returns>Column object</returns>
        public Column Over(WindowSpec window)
        {
            return ApplyMethod("over", window);
        }

        /// <summary>
        /// Defines an empty analytic clause. In this case the analytic function is applied
        /// and presented for all rows in the result set.
        /// </summary>
        /// <returns>Column object</returns>
        public Column Over()
        {
            return ApplyMethod("over");
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params string[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params int[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params long[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params bool[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params short[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params float[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params double[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        ///  A boolean expression that is evaluated to true if the value of this expression 
        ///  is contained by the evaluated values of the arguments.  
        /// </summary>
        /// <param name="list">List of values to check the column against</param>
        /// <returns>Column object</returns>
        public Column IsIn(params decimal[] list)
        {
            return ApplyMethod("isin", list);
        }

        /// <summary>
        /// Gets the underlying Expression object of the <see cref="Column"/>.
        /// </summary>
        internal JvmObjectReference Expr()
        {
            return (JvmObjectReference)Reference.Invoke("expr");
        }

        // Equals() and GetHashCode() are required to be defined when operator==/!=
        // are overloaded.

        /// <summary>
        /// Checks if the given object is equal to this object.
        /// </summary>
        /// <param name="obj">Object to compare to</param>
        /// <returns>True if the given object is equal to this object</returns>
        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj is Column other && Reference.Equals(other.Reference);
        }

        /// <summary>
        /// Calculates the hash code for this object.
        /// </summary>
        /// <returns>Hash code for this object</returns>
        public override int GetHashCode() => Reference.GetHashCode();

        /// <summary>
        /// Invoke the toString method of the column instance
        /// </summary>
        /// <returns>Column name of this column</returns>
        public override string ToString() => (string)Reference.Invoke("toString");

        /// <summary>
        /// Invokes a method under "org.apache.spark.sql.functions" with the given column.
        /// </summary>
        /// <param name="column">Column to apply function</param>
        /// <param name="name">Name of the function</param>
        /// <returns>New column after applying the function</returns>
        private static Column ApplyFunction(Column column, string name)
        {
            return new Column(
                (JvmObjectReference)column.Reference.Jvm.CallStaticJavaMethod(
                    "org.apache.spark.sql.functions",
                    name,
                    column));
        }

        /// <summary>
        /// Invokes an operator (method name) with the current column.
        /// </summary>
        /// <param name="op">Operator to invoke</param>
        /// <returns>New column after applying the operator</returns>
        private Column ApplyMethod(string op)
        {
            return new Column((JvmObjectReference)Reference.Invoke(op));
        }

        /// <summary>
        /// Invokes an operator (method name) with the current column with other object.
        /// </summary>
        /// <param name="op">Operator to invoke</param>
        /// <param name="other">Object to apply the operator with</param>
        /// <returns>New column after applying the operator</returns>
        private Column ApplyMethod(string op, object other)
        {
            return new Column((JvmObjectReference)Reference.Invoke(op, other));
        }

        /// <summary>
        /// Invokes a method name with the current column with two other objects as parameters.
        /// </summary>
        /// <param name="op">Method to invoke</param>
        /// <param name="other1">Object to apply the method with</param>
        /// <param name="other2">Object to apply the method with</param>
        /// <returns>New column after applying the operator</returns>
        private Column ApplyMethod(string op, object other1, object other2)
        {
            return new Column((JvmObjectReference)Reference.Invoke(op, other1, other2));
        }
    }
}
