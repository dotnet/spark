// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Xunit;
using static Microsoft.Spark.Sql.Functions;
using Column = Microsoft.Spark.Sql.Column;

namespace Microsoft.Spark.E2ETest.ScenarioTests
{
    [Collection("Spark E2E Tests")]
    public class EmailSearchTests
    {
        private readonly SparkSession _spark;

        public EmailSearchTests(SparkFixture fixture)
        {
            _spark = fixture.Spark;
        }

        /// <summary>
        /// This is a mimic of Email Search Top People Reducer.
        /// https://msasg.visualstudio.com/DefaultCollection/Shared%20Data/_search?action=contents&text=TopPeopleReducer&type=code&lp=code-Project&filters=ProjectFilters%7BShared%20Data%7DRepositoryFilters%7BMatrixCompliant%7D&pageSize=25&result=DefaultCollection%2FShared%20Data%2FMatrixCompliant%2FGBmaster%2F%2Fsrc%2FODIN-ML%2FPartner%2FEmailRelevance%2FEmailRelevance%2FEmailRelevanceHelper%2FTopPeopleReducer.cs
        /// </summary>
        [Fact]
        public void TestEmailSearchTopNReducerBasics()
        {
            // Read the sample data.
            DataFrame df = _spark
                .Read()
                .Schema("Id STRING, DisplayName STRING, GivenName STRING, Surname STRING, IMAddress STRING, EmailAddress STRING, RelevanceScore DOUBLE, puser STRING, ptenant STRING")
                .Json($"{TestEnvironment.ResourceDirectory}neighbors.json");

            // Trim the IMAddress column.
            Func<Column, Column> trimIMAddress = Udf<string, string>((str) => str.StartsWith("sip:") ? str.Substring(4) : str);
            df = df.WithColumn("IMAddress", trimIMAddress(df["IMAddress"]));

            // Reduce
            df = df.GroupBy("puser", "ptenant").Agg(CollectList("GivenName").Alias("GivenNames"),
                                                     CollectList("Surname").Alias("Surnames"),
                                                     CollectList("DisplayName").Alias("DisplayNames"),
                                                     CollectList("EmailAddress").Alias("EmailAddresses"),
                                                     CollectList("RelevanceScore").Alias("RelevanceScores"),
                                                     CollectList("IMAddress").Alias("IMAddresses"));
            // Format the output.
            df = df.Select(df["puser"],
                      df["ptenant"],
                      ConcatWs(";", df["GivenNames"]).Alias("GivenNames"),
                      ConcatWs(";", df["Surnames"]).Alias("Surnames"),
                      ConcatWs(";", df["DisplayNames"]).Alias("DisplayNames"),
                      ConcatWs(";", df["EmailAddresses"]).Alias("EmailAddresses"),
                      ConcatWs(";", df["RelevanceScores"]).Alias("RelevanceScores"),
                      ConcatWs(";", df["IMAddresses"]).Alias("IMAddresses"));

            Assert.Equal(2, df.Count());
            foreach (Row row in df.Collect())
            {
                string puser = row.GetAs<string>("puser");
                Assert.Equal("MSFT", row.GetAs<string>("ptenant"));
                Assert.Equal("1101.0;900.0;857.0", row.GetAs<string>("RelevanceScores"));
                switch (puser)
                {
                    case "ruih":
                        Assert.Equal("AliceFN;BobFN;CharlieFN", row.GetAs<string>("GivenNames"));
                        Assert.Equal("AliceLN;BobLN;CharlieLN", row.GetAs<string>("Surnames"));
                        Assert.Equal("AliceFN AliceLN;BobFN BobLN;CharlieFN CharlieLN", row.GetAs<string>("DisplayNames"));
                        Assert.Equal("alice@microsoft.com;bob@microsoft.com;charlie@microsoft.com", row.GetAs<string>("EmailAddresses"));
                        Assert.Equal("alice@microsoft.com;bob@microsoft.com;charlie@microsoft.com", row.GetAs<string>("IMAddresses"));
                        break;
                    case "rui":
                        Assert.Equal("DougFN;ElvaFN;FrankFN", row.GetAs<string>("GivenNames"));
                        Assert.Equal("DougLN;ElvaLN;FrankLN", row.GetAs<string>("Surnames"));
                        Assert.Equal("DougFN DougLN;ElvaFN ElvaLN;FrankFN FrankLN", row.GetAs<string>("DisplayNames"));
                        Assert.Equal("doug@microsoft.com;elva@microsoft.com;frank@microsoft.com", row.GetAs<string>("EmailAddresses"));
                        Assert.Equal("doug@microsoft.com;elva@microsoft.com;frank@microsoft.com", row.GetAs<string>("IMAddresses"));
                        break;
                    default:
                        throw new Exception($"Unexpected age: {puser}.");
                }
            }
        }

        /// <summary>
        /// This is a mimic of Email Search Success Action Reducer.
        /// https://msasg.visualstudio.com/DefaultCollection/Shared%20Data/_git/MatrixCompliant?path=%2Fsrc%2FODIN-ML%2FPartner%2FEmailRelevance%2FImpressionView%2FAnalysisTools%2FReducer%2FSearchActionSuccessReducer.cs&_a=contents&version=GBmaster
        /// </summary>
        [Fact]
        public void TestEmailSearchSuccessActionReducerBasics()
        {
            // Read the sample data.
            DataFrame df = _spark.Read().Json($"{TestEnvironment.ResourceDirectory}search_actions.json");

            // Select the required columns.
            df = df.Select("ImpressionId", "ConversationId", "EntityType", "FolderIdList", "ReferenceIdList", "ItemIdList", "ItemImmutableIdList");

            // Convert columns of concatenated string to array of strings.
            Func<Column, Column> toStringArrayUdf = Udf<string, string[]>((str) => str.Split(';'));
            df = df.WithColumn("FolderIdList", toStringArrayUdf(df["FolderIdList"]))
                    .WithColumn("ReferenceIdList", toStringArrayUdf(df["ReferenceIdList"]))
                    .WithColumn("ItemIdList", toStringArrayUdf(df["ItemIdList"]))
                    .WithColumn("ItemImmutableIdList", toStringArrayUdf(df["ItemImmutableIdList"]));

            // Apply the ArrayZip function to combine the i-th element of each array.
            df = df.Select(df["ConversationId"], df["ImpressionId"], df["EntityType"], ArraysZip(df["FolderIdList"], df["ReferenceIdList"], df["ItemIdList"], df["ItemImmutableIdList"]).Alias("ConcatedColumn"));

            // Apply the Explode function to split into multiple rows.
            df = df.Select(df["ConversationId"], df["ImpressionId"], df["EntityType"], Explode(df["ConcatedColumn"]).Alias("NewColumn"));

            // Create multiple columns.
            df = df.WithColumn("FolderId", df["NewColumn"].GetField("FolderIdList"))
                .WithColumn("ReferenceId", df["NewColumn"].GetField("ReferenceIdList"))
                .WithColumn("ItemId", df["NewColumn"].GetField("ItemIdList"))
                .WithColumn("ItemImmutableId", df["NewColumn"].GetField("ItemImmutableIdList"))
                .Select("ConversationId", "ImpressionId", "EntityType", "FolderId", "ItemId", "ReferenceId", "ItemImmutableId");

            // Check the results.
            Assert.Equal(3, df.Count());
            int i = 0;
            foreach (Row row in df.Collect())
            {
                string impressionId = row.GetAs<string>("ImpressionId");
                string conversationId = row.GetAs<string>("ConversationId");
                string entityType = row.GetAs<string>("EntityType");
                Assert.Equal("Imp1", impressionId);
                Assert.Equal("DD8A6B40-B4C9-426F-8194-895E9053077C", conversationId);
                Assert.Equal("Message", entityType);
                string folderId = row.GetAs<string>("FolderId");
                string itemId = row.GetAs<string>("ItemId");
                string referenceId = row.GetAs<string>("ReferenceId");
                string itemImmutableId = row.GetAs<string>("ItemImmutableId");
                if (i == 0)
                {
                    Assert.Equal("F1", folderId);
                    Assert.Equal("ItemId1", itemId);
                    Assert.Equal("R1", referenceId);
                    Assert.Equal("ItemImmutableId1", itemImmutableId);
                }
                else if (i == 1)
                {
                    Assert.Equal("F2", folderId);
                    Assert.Equal("ItemId2", itemId);
                    Assert.Equal("R2", referenceId);
                    Assert.Equal("ItemImmutableId2", itemImmutableId);
                }
                else if (i == 2)
                {
                    Assert.Equal("F3", folderId);
                    Assert.Equal("ItemId3", itemId);
                    Assert.Equal("R3", referenceId);
                    Assert.Equal("ItemImmutableId3", itemImmutableId);
                }
                else
                {
                    throw new Exception(string.Format("Unexpected row: ConversationId={0}, ImpressionId={1}", conversationId, impressionId));
                }

                i++;
            }
        }
    }
}
