// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Diagnostics.CodeAnalysis;

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// This constructor can create various datetime related objects.
/// </summary>
[SuppressMessage("ReSharper", "MergeCastWithTypeCheck")]
[SuppressMessage("ReSharper", "SuggestBaseTypeForParameter")]
internal class DateTimeConstructor : IObjectConstructor {
	
	public enum PythonType {
		DateTime,
		Date,
		Time,
		TimeDelta
	}

	private readonly PythonType _pythontype;

	public DateTimeConstructor(PythonType pythontype) {
		_pythontype = pythontype;
	}

	public object construct(object[] args) {
		switch(_pythontype)
		{
			case PythonType.Date:
				return CreateDate(args);
			case PythonType.Time:
				return CreateTime(args);
			case PythonType.DateTime:
				return CreateDateTime(args);
			case PythonType.TimeDelta:
				return CreateTimedelta(args);
			default:
				throw new PickleException("invalid object type");
		}
	}

	private static TimeSpan CreateTimedelta(object[] args) {
		// python datetime.timedelta -> TimeSpan
		// args is a tuple of 3 ints: days,seconds,microseconds
		if (args.Length != 3)
			throw new PickleException("invalid pickle data for timedelta; expected 3 args, got "+args.Length);
		int days=Convert.ToInt32(args[0]);
		int seconds=Convert.ToInt32(args[1]);
		int micro=Convert.ToInt32(args[2]);
		return new TimeSpan(days, 0, 0, seconds, micro/1000);
	}

	private static DateTime CreateDateTime(object[] args) {
		// python datetime.time --> DateTime
		// args is 10 bytes: yhi, ylo, month, day, hour, minute, second, ms1, ms2, ms3
		// (can be String or byte[])
		// alternate constructor is with 7 integer arguments: year, month, day, hour, minute, second, microseconds
		int yhi, ylo;
		int month, day, hour, minute, second, microsec;

		if(args.Length==7)
		{
			int year = (int)args[0];
			month = (int)args[1];
			day = (int)args[2];
			hour = (int)args[3];
			minute = (int)args[4];
			second = (int)args[5];
			microsec = (int)args[6];
			
			return new DateTime(year, month, day, hour, minute, second, microsec/1000);
		}
		if (args.Length != 1)
			throw new PickleException("invalid pickle data for datetime; expected 1 or 7 args, got "+args.Length);
		
		if(args[0] is string) {
			string parameters = (string) args[0];
			if (parameters.Length != 10)
				throw new PickleException("invalid pickle data for datetime; expected arg of length 10, got length "+parameters.Length);
			yhi = parameters[0];
			ylo = parameters[1];
			month = parameters[2];
			day = parameters[3];
			hour = parameters[4];
			minute = parameters[5];
			second = parameters[6];
			int ms1 = parameters[7];
			int ms2 = parameters[8];
			int ms3 = parameters[9];
			microsec = ((ms1 << 8) | ms2) << 8 | ms3;
		} else {
			var parameters=(byte[])args[0];
			if (parameters.Length != 10)
				throw new PickleException("invalid pickle data for datetime; expected arg of length 10, got length "+parameters.Length);
			yhi=parameters[0]&0xff;
			ylo=parameters[1]&0xff;
			month=parameters[2]&0xff;
			day=parameters[3]&0xff;
			hour=parameters[4]&0xff;
			minute=parameters[5]&0xff;
			second=parameters[6]&0xff;
			int ms1 = parameters[7]&0xff;
			int ms2 = parameters[8]&0xff;
			int ms3 = parameters[9]&0xff;
			microsec = ((ms1 << 8) | ms2) << 8 | ms3;
		}
		return new DateTime(yhi * 256 + ylo, month, day, hour, minute, second, microsec/1000);
	}

	private static TimeSpan CreateTime(object[] args) {
		// python datetime.time --> TimeSpan since midnight
		// args is 6 bytes: hour, minute, second, ms1,ms2,ms3  (String or byte[])
		// alternate constructor passes 4 integers args: hour, minute, second, microsecond)
		int hour, minute, second, microsec;
		if (args.Length==4)
		{
			hour = (int) args[0];
			minute = (int) args[1];
			second = (int) args[2];
			microsec = (int) args[3];
			return new TimeSpan(0, hour, minute, second, microsec/1000);
		}
		if (args.Length != 1)
			throw new PickleException("invalid pickle data for time; expected 1 or 4 args, got "+args.Length);
		if(args[0] is string) {
			string parameters = (string) args[0];
			if (parameters.Length != 6)
				throw new PickleException("invalid pickle data for time; expected arg of length 6, got length "+parameters.Length);
			hour = parameters[0];
			minute = parameters[1];
			second = parameters[2];
			int ms1 = parameters[3];
			int ms2 = parameters[4];
			int ms3 = parameters[5];
			microsec = ((ms1 << 8) | ms2) << 8 | ms3;
		} else {
			var parameters=(byte[])args[0];
			if (parameters.Length != 6)
				throw new PickleException("invalid pickle data for datetime; expected arg of length 6, got length "+parameters.Length);
			hour=parameters[0]&0xff;
			minute=parameters[1]&0xff;
			second=parameters[2]&0xff;
			int ms1 = parameters[3]&0xff;
			int ms2 = parameters[4]&0xff;
			int ms3 = parameters[5]&0xff;
			microsec = ((ms1 << 8) | ms2) << 8 | ms3;
		}
		return new TimeSpan(0, hour, minute, second, microsec/1000);
	}

	private static DateTime CreateDate(object[] args) {
		// python datetime.date --> DateTime
		// args is a string of 4 bytes yhi, ylo, month, day (String or byte[])
		if (args.Length != 1)
			throw new PickleException("invalid pickle data for date; expected 1 arg, got "+args.Length);
		int yhi, ylo, month, day;
		if(args[0] is string) {
			string parameters = (string) args[0];
			if (parameters.Length != 4)
				throw new PickleException("invalid pickle data for date; expected arg of length 4, got length "+parameters.Length);
			yhi = parameters[0];
			ylo = parameters[1];
			month = parameters[2];
			day = parameters[3];
		} else {
			var parameters=(byte[])args[0];
			if (parameters.Length != 4)
				throw new PickleException("invalid pickle data for date; expected arg of length 4, got length "+parameters.Length);
			yhi=parameters[0]&0xff;
			ylo=parameters[1]&0xff;
			month=parameters[2]&0xff;
			day=parameters[3]&0xff;
		}
		return new DateTime(yhi*256+ylo, month, day);
	}
}

}
