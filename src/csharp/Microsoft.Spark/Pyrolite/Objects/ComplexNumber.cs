// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;
// ReSharper disable CompareOfFloatsByEqualityOperator
// ReSharper disable NonReadonlyMemberInGetHashCode
// ReSharper disable UnusedMember.Global

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// An immutable Complex Number class. 
/// </summary>
[SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
internal class ComplexNumber {
	
	public double Real {get; }
	public double Imaginary {get; }
	
	public ComplexNumber(double r, double i) {
		Real=r;
		Imaginary=i;
	}

	public override string ToString()
	{
		StringBuilder sb=new StringBuilder();
		sb.Append(Real);
		if(Imaginary>0)
			sb.Append('+');
		return sb.Append(Imaginary).Append('i').ToString();
	}

	public double Magnitude() {
		return Math.Sqrt(Real * Real + Imaginary * Imaginary);
	}

	public static ComplexNumber operator +(ComplexNumber c1, ComplexNumber c2) {
		return new ComplexNumber(c1.Real + c2.Real, c1.Imaginary + c2.Imaginary);
	}


	public static ComplexNumber operator -(ComplexNumber c1, ComplexNumber c2) {
		return new ComplexNumber(c1.Real - c2.Real, c1.Imaginary - c2.Imaginary);
	}

	public static ComplexNumber operator *(ComplexNumber c1, ComplexNumber c2) {
		return new ComplexNumber(c1.Real * c2.Real - c1.Imaginary * c2.Imaginary, c1.Real * c2.Imaginary + c1.Imaginary * c2.Real);
	}

	public static ComplexNumber operator /(ComplexNumber c1, ComplexNumber c2) {
		return new ComplexNumber((c1.Real * c2.Real + c1.Imaginary * c2.Imaginary) / (c2.Real * c2.Real + c2.Imaginary * c2.Imaginary), (c1.Imaginary * c2.Real - c1.Real * c2.Imaginary)
				/ (c2.Real * c2.Real + c2.Imaginary * c2.Imaginary));
	}

	#region Equals and GetHashCode implementation
	public override bool Equals(object obj)
	{
		ComplexNumber other = obj as ComplexNumber;
		if (other == null)
			return false;
		return Real==other.Real && Imaginary==other.Imaginary;
	}
	
	public override int GetHashCode()
	{
		return Real.GetHashCode() ^ Imaginary.GetHashCode();
	}
	
	public static bool operator ==(ComplexNumber lhs, ComplexNumber rhs)
	{
		if (ReferenceEquals(lhs, rhs))
			return true;
		if (ReferenceEquals(lhs, null) || ReferenceEquals(rhs, null))
			return false;
		return lhs.Equals(rhs);
	}
	
	public static bool operator !=(ComplexNumber lhs, ComplexNumber rhs)
	{
		return !(lhs == rhs);
	}
	#endregion
}

}
