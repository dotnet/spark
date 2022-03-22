// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using System.Reflection;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// Params is used for components that take parameters. This also provides
    /// an internal param map to store parameter values attached to the instance.
    /// An abstract class corresponds to scala's Params trait.
    /// </summary>
    public abstract class Params : Identifiable, IJvmObjectReferenceProvider
    {
        internal Params(string className)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className))
        {
        }

        internal Params(string className, string uid)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className, uid))
        {
        }

        internal Params(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Returns the JVM toString value rather than the .NET ToString default
        /// </summary>
        /// <returns>JVM toString() value</returns>
        public override string ToString() => (string)Reference.Invoke("toString");

        /// <summary>
        /// The UID that was used to create the object. If no UID is passed in when creating the
        /// object then a random UID is created when the object is created.
        /// </summary>
        /// <returns>string UID identifying the object</returns>
        public string Uid() => (string)Reference.Invoke("uid");

        /// <summary>
        /// Returns a description of how a specific <see cref="Param"/> works and is currently set.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to explain</param>
        /// <returns>Description of the <see cref="Param"/></returns>
        public string ExplainParam(Param.Param param) =>
            (string)Reference.Invoke("explainParam", param);

        /// <summary>
        /// Returns a description of how all of the <see cref="Param"/>'s that apply to this object
        /// work and how they are currently set.
        /// </summary>
        /// <returns>Description of all the applicable <see cref="Param"/>'s</returns>
        public string ExplainParams() => (string)Reference.Invoke("explainParams");

        /// <summary>Checks whether a param is explicitly set.</summary>
        /// <param name="param">The <see cref="Param"/> to be checked.</param>
        /// <returns>bool</returns>
        public bool IsSet(Param.Param param) => (bool)Reference.Invoke("isSet", param);

        /// <summary>Checks whether a param is explicitly set or has a default value.</summary>
        /// <param name="param">The <see cref="Param"/> to be checked.</param>
        /// <returns>bool</returns>
        public bool IsDefined(Param.Param param) => (bool)Reference.Invoke("isDefined", param);

        /// <summary>
        /// Tests whether this instance contains a param with a given name.
        /// </summary>
        /// <param name="paramName">The <see cref="Param"/> to be test.</param>
        /// <returns>bool</returns>
        public bool HasParam(string paramName) => (bool)Reference.Invoke("hasParam", paramName);

        /// <summary>
        /// Retrieves a <see cref="Param"/> so that it can be used to set the value of the
        /// <see cref="Param"/> on the object.
        /// </summary>
        /// <param name="paramName">The name of the <see cref="Param"/> to get.</param>
        /// <returns><see cref="Param"/> that can be used to set the actual value</returns>
        public Param.Param GetParam(string paramName) =>
            new Param.Param((JvmObjectReference)Reference.Invoke("getParam", paramName));

        /// <summary>
        /// Sets the value of a specific <see cref="Param"/>.
        /// </summary>
        /// <param name="param"><see cref="Param"/> to set the value of</param>
        /// <param name="value">The value to use</param>
        /// <returns>The object that contains the newly set <see cref="Param"/></returns>
        public T Set<T>(Param.Param param, object value) =>
            WrapAsType<T>((JvmObjectReference)Reference.Invoke("set", param, value));

        /// <summary>
        /// Clears any value that was previously set for this <see cref="Param"/>. The value is
        /// reset to the default value.
        /// </summary>
        /// <param name="param">The <see cref="Param"/> to set back to its original value</param>
        /// <returns>Object reference that was used to clear the <see cref="Param"/></returns>
        public T Clear<T>(Param.Param param) =>
            WrapAsType<T>((JvmObjectReference)Reference.Invoke("clear", param));

        protected static T WrapAsType<T>(JvmObjectReference reference)
        {
            ConstructorInfo constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .Single(c =>
                {
                    ParameterInfo[] parameters = c.GetParameters();
                    return (parameters.Length == 1) &&
                        (parameters[0].ParameterType == typeof(JvmObjectReference));
                });

            return (T)constructor.Invoke(new object[] { reference });
        }
    }

    /// <summary>
    /// <see cref="ScalaPipelineStage"/> A stage in a pipeline, either an Estimator or a Transformer.
    /// </summary>
    public abstract class ScalaPipelineStage : Params
    {
        internal ScalaPipelineStage(string className) : base(className)
        {
        }

        internal ScalaPipelineStage(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaPipelineStage(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Check transform validity and derive the output schema from the input schema.
        ///
        /// We check validity for interactions between parameters during transformSchema
        /// and raise an exception if any parameter value is invalid.
        ///
        /// Typical implementation should first conduct verification on schema change and
        /// parameter validity, including complex parameter interaction checks.
        /// </summary>
        /// <param name="schema">
        /// The <see cref="StructType"/> of the <see cref="DataFrame"/> which will be transformed.
        /// </param>
        /// <returns>
        /// The <see cref="StructType"/> of the output schema that would have been derived from the
        /// input schema, if Transform had been called.
        /// </returns>
        public virtual StructType TransformSchema(StructType schema) =>
             new StructType(
                (JvmObjectReference)Reference.Invoke(
                    "transformSchema",
                    DataType.FromJson(Reference.Jvm, schema.Json)));
    }

    /// <summary>
    /// <see cref="ScalaTransformer"/> Abstract class for transformers that transform one dataset into another.
    /// </summary>
    public abstract class ScalaTransformer : ScalaPipelineStage
    {
        internal ScalaTransformer(string className) : base(className)
        {
        }

        internal ScalaTransformer(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaTransformer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Executes the transformer and transforms the DataFrame to include new columns.
        /// </summary>
        /// <param name="dataset">The Dataframe to be transformed.</param>
        /// <returns>
        /// <see cref="DataFrame"/> containing the original data and new columns.
        /// </returns>
        public virtual DataFrame Transform(DataFrame dataset) =>
            new DataFrame((JvmObjectReference)Reference.Invoke("transform", dataset));
    }

    /// <summary>
    /// A helper interface for ScalaEstimator, so that when we have an array of ScalaEstimators
    /// with different type params, we can hold all of them with Estimator&lt;object&gt;.
    /// </summary>
    public interface Estimator<out M>
    {
        M Fit(DataFrame dataset);
    }

    /// <summary>
    /// Abstract Class for estimators that fit models to data.
    /// </summary>
    /// <typeparam name="M"/>
    public abstract class ScalaEstimator<M> : ScalaPipelineStage, Estimator<M> where M : ScalaModel<M>
    {
        internal ScalaEstimator(string className) : base(className)
        {
        }

        internal ScalaEstimator(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaEstimator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Fits a model to the input data.
        /// </summary>
        /// <param name="dataset">input dataset.</param>
        /// <returns>fitted model</returns>
        public virtual M Fit(DataFrame dataset) =>
            WrapAsType<M>((JvmObjectReference)Reference.Invoke("fit", dataset));
    }

    /// <summary>
    /// A helper interface for ScalaModel, so that when we have an array of ScalaModels
    /// with different type params, we can hold all of them with Model&lt;object&gt;.
    /// </summary>
    public interface Model<out M>
    {
        bool HasParent();
    }

    /// <summary>
    /// A fitted model, i.e., a Transformer produced by an Estimator.
    /// </summary>
    /// <typeparam name="M">
    /// Model Type.
    /// </typeparam>
    public abstract class ScalaModel<M> : ScalaTransformer, Model<M> where M : ScalaModel<M>
    {
        internal ScalaModel(string className) : base(className)
        {
        }

        internal ScalaModel(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets the parent of this model (Java API).
        /// </summary>
        /// <returns>type parameter M</returns>
        public M SetParent(ScalaEstimator<M> parent) =>
            WrapAsType<M>((JvmObjectReference)Reference.Invoke("setParent", parent));

        /// <summary>
        /// Indicates whether this Model has a corresponding parent.
        /// </summary>
        /// <returns>bool</returns>
        public bool HasParent() =>
            (bool)Reference.Invoke("hasParent");
    }

    /// <summary>
    /// <see cref="ScalaEvaluator"/> Abstract Class for evaluators that compute metrics from predictions.
    /// </summary>
    public abstract class ScalaEvaluator : Params
    {
        internal ScalaEvaluator(string className) : base(className)
        {
        }

        internal ScalaEvaluator(string className, string uid) : base(className, uid)
        {
        }

        internal ScalaEvaluator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Evaluates model output and returns a scalar metric.
        /// The value of isLargerBetter specifies whether larger values are better.
        /// </summary>
        /// <param name="dataset">a dataset that contains labels/observations and predictions.</param>
        /// <returns>metric</returns>
        public virtual double Evaluate(DataFrame dataset) =>
            (double)Reference.Invoke("evaluate", dataset);

        /// <summary>
        /// Indicates whether the metric returned by evaluate should be maximized (true, default) or minimized (false).
        /// A given evaluator may support multiple metrics which may be maximized or minimized.
        /// </summary>
        /// <returns>bool</returns>
        public bool IsLargerBetter =>
            (bool)Reference.Invoke("isLargerBetter");
    }

    /// <summary>
    /// DotnetHelper is used to hold basic general helper functions that
    /// are used within ML scope.
    /// </summary>
    public class DotnetHelper
    {
        /// <summary>
        /// Helper function for getting the exact class name from jvm object.
        /// </summary>
        /// <param name="jvmObject">The reference to object created in JVM.</param>
        /// <returns>A string Tuple2 of constructor class name and method name</returns>
        public static (string, string) GetUnderlyingType(JvmObjectReference jvmObject)
        {
            JvmObjectReference jvmClass = (JvmObjectReference)jvmObject.Invoke("getClass");
            string returnClass = (string)jvmClass.Invoke("getTypeName");
            var dotnetClass = returnClass.Replace("com.microsoft.azure.synapse.ml", "Synapse.ML")
                .Replace("org.apache.spark.ml", "Microsoft.Spark.ML")
                .Split(".".ToCharArray());
            var renameClass = dotnetClass.Select(x => char.ToUpper(x[0]) + x.Substring(1)).ToArray();
            string constructorClass = string.Join(".", renameClass);
            string methodName = "WrapAs" + dotnetClass[dotnetClass.Length - 1];
            return (constructorClass, methodName);
        }
    }
}
