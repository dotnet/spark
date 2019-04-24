// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Reflection;

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// This creates Python Exception instances. 
/// It keeps track of the original Python exception type name as well.
/// </summary>
internal class ExceptionConstructor : IObjectConstructor {

	private readonly string _pythonExceptionType;
	private readonly Type _type;
	
	public ExceptionConstructor(Type type, string module, string name) {
		if(!string.IsNullOrEmpty(module))
			_pythonExceptionType = module+"."+name;
		else
			_pythonExceptionType = name;
		_type = type;
	}

	public object construct(object[] args) {
		try {
			if(!string.IsNullOrEmpty(_pythonExceptionType)) {
				// put the python exception type somewhere in the message
				if(args==null || args.Length==0) {
					args = new object[] { "["+_pythonExceptionType+"]" };
				} else {
					string msg = (string)args[0];
					msg = $"[{_pythonExceptionType}] {msg}";
					args = new object[] {msg};
				}
			}
			object ex = Activator.CreateInstance(_type, args);
			
			PropertyInfo prop=ex.GetType().GetProperty("PythonExceptionType");
			if(prop!=null) {
				prop.SetValue(ex, _pythonExceptionType, null);
			}
			return ex;
		} catch (Exception x) {
			throw new PickleException("problem constructing object",x);
		}
	}
}

}
