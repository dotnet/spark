#if NETFRAMEWORK
// C# 9 init-only setters require this type; .NET Framework doesn't include it.
namespace System.Runtime.CompilerServices
{
    internal class IsExternalInit { }
}
#endif
