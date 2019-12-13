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
            // Arrange
            var testJson = GetTestJson();
            using (var document = JsonDocument.Parse(testJson))
            {
                var testElement = document.RootElement;
                var expectedJson = GetExpectedSortedJson();

                // Act
                var resultElement = testElement.SortProperties();

                // Assert
                Assert.Equal(expectedJson, resultElement);
            }
        }

        private string GetTestJson()
        {
            var obj = new
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
            return JsonSerializer.Serialize(obj);
        }

        private string GetExpectedSortedJson()
        {
            var obj = new
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
            return JsonSerializer.Serialize(obj);
        }
    }
}
