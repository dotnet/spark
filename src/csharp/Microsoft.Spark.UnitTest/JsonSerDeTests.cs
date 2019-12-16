// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Text.Json;
using Microsoft.Spark.Interop.Ipc;
using Xunit;

namespace Microsoft.Spark.UnitTest
{
    public class JsonSerDeTests
    {
        [Fact]
        public void TestSortProperties()
        {
            var (testJson, expectedJson) = GetTestExpectedJson();
            using (var document = JsonDocument.Parse(testJson))
            {
                Assert.Equal(expectedJson, document.RootElement.SortProperties());
            }
        }

        private (string TestJson, string ExpectedJson) GetTestExpectedJson()
        {
            var testObj = new
            {
                objC = new
                {
                    propC = "valueC",
                    propB = "valueB",
                    propA = "valueA"
                },
                objB = new
                {
                    propC = "valueC",
                    propB = "valueB",
                    propA = "valueA"
                },
                objA = new
                {
                    propC = "valueC",
                    propB = "valueB",
                    propA = "valueA"
                },
                arrayC = new[] { 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }, 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }, 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }                                        
                },
                arrayB = new[] { 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }, 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }, 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }                                        
                },
                arrayA = new[] { 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }, 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }, 
                    new {
                        propC = "valueC",
                        propB = "valueB",
                        propA = "valueA"
                    }                                        
                }                                
            };
            var expectedJson = new
            {
                arrayA = new[] { 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }, 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }, 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }                                        
                },                
                arrayB = new[] { 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }, 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }, 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }                                              
                },
                arrayC = new[] { 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }, 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }, 
                    new {
                        propA = "valueA",  
                        propB = "valueB",                                              
                        propC = "valueC"
                    }                                             
                },                
                objA = new
                {
                    propA = "valueA",
                    propB = "valueB",                                        
                    propC = "valueC"
                },
                objB = new
                {
                    propA = "valueA",
                    propB = "valueB",                                        
                    propC = "valueC"
                },
                objC = new
                {
                    propA = "valueA",
                    propB = "valueB",                                        
                    propC = "valueC"
                }                                        
            };            
            return (JsonSerializer.Serialize(testObj), JsonSerializer.Serialize(expectedJson));
        }
    }
}
